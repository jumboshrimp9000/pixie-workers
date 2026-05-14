import dotenv from "dotenv";
import { createRequire } from "module";

const requireFromBackend = createRequire("/Users/omermullick/Downloads/Projects/AP/backend/package.json");
const axios = requireFromBackend("axios");
const { createClient } = requireFromBackend("@supabase/supabase-js");

dotenv.config({ path: "/Users/omermullick/Downloads/Projects/AP/.env" });

const BATCH_ID = "ffa54dd8-3fd1-4336-8e06-6f73872432f4";
const TAG_LABEL = "Mailboxpro 5/10";
const ASSIGN_CHUNK_SIZE = Math.max(1, Number(process.env.INSTANTLY_TAG_ASSIGN_CHUNK_SIZE || 100));
const ASSIGN_CONCURRENCY = Math.max(1, Number(process.env.INSTANTLY_TAG_ASSIGN_CONCURRENCY || 4));

const supabaseUrl = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!supabaseUrl || !supabaseKey) throw new Error("Missing Supabase env");

const db = createClient(supabaseUrl, supabaseKey, {
  auth: { persistSession: false, autoRefreshToken: false },
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const normalizeEmail = (value) => String(value || "").trim().toLowerCase();
const authHeaders = (apiKey) => ({ Authorization: `Bearer ${apiKey}` });

async function fetchAll(builder, pageSize = 1000) {
  const rows = [];
  for (let from = 0; ; from += pageSize) {
    const { data, error } = await builder.range(from, from + pageSize - 1);
    if (error) throw error;
    rows.push(...(data || []));
    if (!data || data.length < pageSize) break;
  }
  return rows;
}

async function fetchActiveDomains() {
  return fetchAll(
    db
      .from("domains")
      .select("id,domain,status")
      .eq("order_batch_id", BATCH_ID)
      .eq("status", "active")
      .order("domain")
  );
}

async function fetchActiveInboxes(domainIds) {
  const rows = [];
  for (let i = 0; i < domainIds.length; i += 100) {
    rows.push(...await fetchAll(
      db
        .from("inboxes")
        .select("domain_id,email")
        .in("domain_id", domainIds.slice(i, i + 100))
        .eq("status", "active")
        .order("email")
    ));
  }
  return rows;
}

async function fetchInstantlyApiKey(domainId) {
  const { data, error } = await db
    .from("domain_credentials")
    .select("sending_tool_credentials(api_key,sending_tools(slug))")
    .eq("domain_id", domainId)
    .limit(10);
  if (error) throw error;
  const instantly = (data || []).find((row) => {
    const slug = String(row?.sending_tool_credentials?.sending_tools?.slug || "").trim().toLowerCase();
    return slug === "instantly" || slug === "instantly.ai";
  });
  const apiKey = String(instantly?.sending_tool_credentials?.api_key || "").trim();
  if (!apiKey) throw new Error("Missing Instantly API key for ProfitPath domains");
  return apiKey;
}

async function fetchCustomTags(apiKey, search = "") {
  const rows = [];
  let startingAfter = "";
  for (let page = 1; page <= 100; page += 1) {
    const params = { limit: 100 };
    if (startingAfter) params.starting_after = startingAfter;
    if (search) params.search = search;
    const response = await axios.get("https://api.instantly.ai/api/v2/custom-tags", {
      headers: authHeaders(apiKey),
      params,
      timeout: 20000,
    });
    const pageRows = Array.isArray(response.data?.items)
      ? response.data.items
      : Array.isArray(response.data?.data)
        ? response.data.data
        : Array.isArray(response.data)
          ? response.data
          : [];
    rows.push(...pageRows);
    startingAfter = String(response.data?.next_starting_after || "").trim();
    if (!startingAfter) break;
  }
  return rows
    .map((row) => ({
      id: String(row?.id || row?.tag_id || "").trim(),
      label: String(row?.label || row?.name || row?.title || "").trim(),
    }))
    .filter((row) => row.id && row.label);
}

async function findOrCreateTag(apiKey) {
  const normalized = TAG_LABEL.toLowerCase();
  const existing = (await fetchCustomTags(apiKey, TAG_LABEL))
    .find((tag) => tag.label.trim().toLowerCase() === normalized);
  if (existing?.id) return existing.id;

  const payloads = [
    { label: TAG_LABEL, color: "#64748B" },
    { label: TAG_LABEL },
    { name: TAG_LABEL },
  ];
  let lastError = "";
  for (const payload of payloads) {
    try {
      const response = await axios.post("https://api.instantly.ai/api/v2/custom-tags", payload, {
        headers: authHeaders(apiKey),
        timeout: 20000,
      });
      const id = String(response.data?.id || response.data?.tag_id || "").trim();
      if (id) return id;
    } catch (err) {
      lastError = String(err?.response?.data?.message || err?.response?.data?.error || err?.message || "tag create failed");
    }
  }

  const refreshed = (await fetchCustomTags(apiKey, TAG_LABEL))
    .find((tag) => tag.label.trim().toLowerCase() === normalized);
  if (refreshed?.id) return refreshed.id;
  throw new Error(lastError || `Failed to create tag ${TAG_LABEL}`);
}

async function fetchAccountsForDomain(apiKey, domain) {
  const accounts = new Set();
  let startingAfter = "";
  for (let page = 1; page <= 20; page += 1) {
    const params = { limit: 100, search: domain };
    if (startingAfter) params.starting_after = startingAfter;
    const response = await axios.get("https://api.instantly.ai/api/v2/accounts", {
      headers: authHeaders(apiKey),
      params,
      timeout: 20000,
    });
    const rows = Array.isArray(response.data?.items)
      ? response.data.items
      : Array.isArray(response.data?.data)
        ? response.data.data
        : Array.isArray(response.data)
          ? response.data
          : [];
    for (const row of rows) {
      const email = normalizeEmail(row?.email || row?.account_email);
      if (email) accounts.add(email);
    }
    startingAfter = String(response.data?.next_starting_after || "").trim();
    if (!startingAfter) break;
  }
  return accounts;
}

async function toggleTagChunk(apiKey, tagId, emails) {
  const payloads = [
    { resource_type: 1, resource_ids: emails, tag_ids: [tagId], assign: true },
    { resource_type: "account", resource_ids: emails, tag_ids: [tagId], assign: true },
  ];
  let lastError = "";
  for (const payload of payloads) {
    for (let attempt = 1; attempt <= 8; attempt += 1) {
      try {
        await axios.post("https://api.instantly.ai/api/v2/custom-tags/toggle-resource", payload, {
          headers: authHeaders(apiKey),
          timeout: 30000,
        });
        return;
      } catch (err) {
        const status = Number(err?.response?.status || 0);
        lastError = String(err?.response?.data?.message || err?.response?.data?.error || err?.message || "tag assignment failed");
        const retryAfter = Number(err?.response?.headers?.["retry-after"] || 0);
        if ((status === 429 || status >= 500) && attempt < 8) {
          await sleep(retryAfter > 0 ? retryAfter * 1000 : Math.min(2000 * attempt, 20000));
          continue;
        }
        break;
      }
    }
  }
  throw new Error(lastError || "tag assignment failed");
}

async function fetchMappedEmailsForTag(apiKey, tagId, targetSet) {
  const mapped = new Set();
  let startingAfter = "";
  for (let page = 1; page <= 500; page += 1) {
    const params = { limit: 100, tag_ids: tagId };
    if (startingAfter) params.starting_after = startingAfter;
    const response = await axios.get("https://api.instantly.ai/api/v2/custom-tag-mappings", {
      headers: authHeaders(apiKey),
      params,
      timeout: 20000,
    });
    const rows = Array.isArray(response.data?.items)
      ? response.data.items
      : Array.isArray(response.data?.data)
        ? response.data.data
        : Array.isArray(response.data)
          ? response.data
          : [];
    for (const row of rows) {
      const rowTagId = String(row?.tag_id || row?.custom_tag_id || "").trim();
      if (rowTagId && rowTagId !== tagId) continue;
      const email = normalizeEmail(row?.resource_id || row?.email || row?.account_email);
      if (targetSet.has(email)) mapped.add(email);
    }
    startingAfter = String(response.data?.next_starting_after || "").trim();
    if (!startingAfter) break;
  }
  const unresolved = Array.from(targetSet).filter((email) => !mapped.has(email));
  await forEachWithConcurrency(unresolved, 8, async (email) => {
    const response = await axios.get("https://api.instantly.ai/api/v2/custom-tag-mappings", {
      headers: authHeaders(apiKey),
      params: { limit: 100, tag_ids: tagId, resource_ids: email },
      timeout: 20000,
    });
    const rows = Array.isArray(response.data?.items)
      ? response.data.items
      : Array.isArray(response.data?.data)
        ? response.data.data
        : Array.isArray(response.data)
          ? response.data
          : [];
    const found = rows.some((row) => {
      const rowTagId = String(row?.tag_id || row?.custom_tag_id || "").trim();
      const rowEmail = normalizeEmail(row?.resource_id || row?.email || row?.account_email);
      return rowEmail === email && (!rowTagId || rowTagId === tagId);
    });
    if (found) mapped.add(email);
  });
  return mapped;
}

async function forEachWithConcurrency(items, concurrency, task) {
  let index = 0;
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (index < items.length) {
      const item = items[index++];
      await task(item);
    }
  });
  await Promise.all(workers);
}

function chunks(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

async function main() {
  const domains = await fetchActiveDomains();
  if (domains.length === 0) {
    console.log(JSON.stringify({ activeDomains: 0 }));
    return;
  }
  const apiKey = await fetchInstantlyApiKey(domains[0].id);
  const tagId = await findOrCreateTag(apiKey);
  const inboxes = await fetchActiveInboxes(domains.map((domain) => domain.id));
  const domainsById = new Map(domains.map((domain) => [domain.id, domain]));
  const targetEmailsByDomain = new Map();
  const targetSet = new Set();
  for (const inbox of inboxes) {
    const domain = domainsById.get(inbox.domain_id);
    const email = normalizeEmail(inbox.email);
    if (!domain || !email) continue;
    if (!targetEmailsByDomain.has(domain.domain)) targetEmailsByDomain.set(domain.domain, new Set());
    targetEmailsByDomain.get(domain.domain).add(email);
    targetSet.add(email);
  }

  const foundSet = new Set();
  const missing = [];
  for (const [domain, targetEmails] of targetEmailsByDomain.entries()) {
    const accounts = await fetchAccountsForDomain(apiKey, domain);
    for (const email of targetEmails) {
      if (accounts.has(email)) foundSet.add(email);
      else missing.push(email);
    }
    if ((foundSet.size + missing.length) % 990 === 0) {
      console.log(JSON.stringify({ stage: "scan", found: foundSet.size, missing: missing.length }));
    }
  }

  let mappedBefore = await fetchMappedEmailsForTag(apiKey, tagId, foundSet);
  let toAssign = Array.from(foundSet).filter((email) => !mappedBefore.has(email));
  const batches = chunks(toAssign, ASSIGN_CHUNK_SIZE);
  console.log(JSON.stringify({
    stage: "assign_start",
    tag: TAG_LABEL,
    tagId,
    activeDomains: domains.length,
    targetInboxes: inboxes.length,
    foundAccounts: foundSet.size,
    missing: missing.length,
    alreadyTagged: mappedBefore.size,
    toAssign: toAssign.length,
    batches: batches.length,
  }));

  let assigned = 0;
  const failures = [];
  await forEachWithConcurrency(batches, ASSIGN_CONCURRENCY, async (batch) => {
    try {
      await toggleTagChunk(apiKey, tagId, batch);
      assigned += batch.length;
      if (assigned % 1000 < ASSIGN_CHUNK_SIZE) {
        console.log(JSON.stringify({ stage: "assign_progress", assigned, remaining: Math.max(0, toAssign.length - assigned), failures: failures.length }));
      }
    } catch (err) {
      failures.push({ sample: batch.slice(0, 3), count: batch.length, error: String(err?.message || err).slice(0, 300) });
    }
  });

  await sleep(5000);
  const mappedAfter = await fetchMappedEmailsForTag(apiKey, tagId, foundSet);
  const untaggedAfter = Array.from(foundSet).filter((email) => !mappedAfter.has(email));
  console.log(JSON.stringify({
    stage: "done",
    tag: TAG_LABEL,
    tagId,
    activeDomains: domains.length,
    targetInboxes: inboxes.length,
    foundAccounts: foundSet.size,
    missing: missing.length,
    alreadyTaggedBefore: mappedBefore.size,
    attemptedAssign: toAssign.length,
    assigned,
    failures: failures.length,
    failureSamples: failures.slice(0, 10),
    taggedAfter: mappedAfter.size,
    untaggedAfter: untaggedAfter.length,
    untaggedSamples: untaggedAfter.slice(0, 20),
  }, null, 2));

  if (missing.length > 0 || failures.length > 0 || untaggedAfter.length > 0) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(JSON.stringify({ stage: "fatal", error: String(err?.message || err) }, null, 2));
  process.exitCode = 1;
});
