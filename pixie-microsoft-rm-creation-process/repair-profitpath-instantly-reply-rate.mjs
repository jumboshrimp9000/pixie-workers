import dotenv from "dotenv";
import { createRequire } from "module";

const requireFromBackend = createRequire("/Users/omermullick/Downloads/Projects/AP/backend/package.json");
const axios = requireFromBackend("axios");
const { createClient } = requireFromBackend("@supabase/supabase-js");

dotenv.config({ path: "../../AP/.env" });
dotenv.config({ path: "/Users/omermullick/Downloads/Projects/AP/.env", override: false });

const BATCH_ID = "ffa54dd8-3fd1-4336-8e06-6f73872432f4";
const TARGET = {
  dailyLimit: 5,
  sendingGap: 30,
  warmupLimit: 5,
  warmupIncrement: "disabled",
  replyRate: 60,
};

const CONCURRENCY = Math.max(1, Number(process.env.INSTANTLY_REPLY_RATE_REPAIR_CONCURRENCY || 6));

const supabaseUrl = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!supabaseUrl || !supabaseKey) {
  throw new Error("Missing Supabase env");
}

const db = createClient(supabaseUrl, supabaseKey, {
  auth: { persistSession: false, autoRefreshToken: false },
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const normalizeEmail = (value) => String(value || "").trim().toLowerCase();
const asNumber = (value) => {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : undefined;
};
const closeNumber = (actual, expected) => {
  const numeric = asNumber(actual);
  return numeric !== undefined && Math.abs(numeric - expected) <= 0.001;
};
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
  if (domainIds.length === 0) return [];
  const rows = [];
  for (let i = 0; i < domainIds.length; i += 100) {
    const chunk = domainIds.slice(i, i + 100);
    rows.push(...await fetchAll(
      db
        .from("inboxes")
        .select("domain_id,email")
        .in("domain_id", chunk)
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

async function fetchAccountsForDomain(apiKey, domain) {
  const accounts = new Map();
  let startingAfter = "";
  let pages = 0;
  while (true) {
    pages += 1;
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
      if (email) accounts.set(email, row);
    }
    startingAfter = String(response.data?.next_starting_after || "").trim();
    if (!startingAfter || pages >= 20) return accounts;
  }
}

function settingsOk(account) {
  const warmup = account?.warmup || {};
  return closeNumber(account?.daily_limit ?? account?.dailyLimit, TARGET.dailyLimit) &&
    closeNumber(account?.sending_gap ?? account?.sendingGap, TARGET.sendingGap) &&
    closeNumber(warmup?.limit ?? warmup?.warmup_daily_limit ?? warmup?.daily_limit, TARGET.warmupLimit) &&
    String(warmup?.increment ?? account?.warmup_increment ?? "").trim().toLowerCase() === TARGET.warmupIncrement &&
    closeNumber(warmup?.reply_rate, TARGET.replyRate);
}

async function patchAccount(apiKey, email) {
  const payload = {
    daily_limit: TARGET.dailyLimit,
    sending_gap: TARGET.sendingGap,
    enable_slow_ramp: false,
    warmup: {
      limit: TARGET.warmupLimit,
      increment: TARGET.warmupIncrement,
      reply_rate: TARGET.replyRate,
    },
  };

  let lastError = "";
  for (let attempt = 1; attempt <= 10; attempt += 1) {
    try {
      await axios.patch(`https://api.instantly.ai/api/v2/accounts/${encodeURIComponent(email)}`, payload, {
        headers: authHeaders(apiKey),
        timeout: 20000,
      });
      return;
    } catch (err) {
      const status = Number(err?.response?.status || 0);
      lastError = String(err?.response?.data?.message || err?.response?.data?.error || err?.message || "patch failed");
      const retryAfter = Number(err?.response?.headers?.["retry-after"] || 0);
      const retryable = status === 429 || status >= 500 || /update-warmup-accounts job in progress/i.test(lastError);
      if (retryable && attempt < 10) {
        await sleep(retryAfter > 0 ? retryAfter * 1000 : Math.min(2500 * attempt, 30000));
        continue;
      }
      throw new Error(lastError);
    }
  }
  throw new Error(lastError || "patch failed");
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

async function main() {
  const domains = await fetchActiveDomains();
  if (domains.length === 0) {
    console.log(JSON.stringify({ activeDomains: 0 }));
    return;
  }
  const apiKey = await fetchInstantlyApiKey(domains[0].id);
  const inboxes = await fetchActiveInboxes(domains.map((domain) => domain.id));
  const domainsById = new Map(domains.map((domain) => [domain.id, domain]));
  const targetEmailsByDomain = new Map();
  for (const inbox of inboxes) {
    const domain = domainsById.get(inbox.domain_id);
    const email = normalizeEmail(inbox.email);
    if (!domain || !email) continue;
    if (!targetEmailsByDomain.has(domain.domain)) targetEmailsByDomain.set(domain.domain, new Set());
    targetEmailsByDomain.get(domain.domain).add(email);
  }

  const toPatch = [];
  const missing = [];
  let alreadyOk = 0;
  let seen = 0;

  for (const [domain, targetEmails] of targetEmailsByDomain.entries()) {
    const accounts = await fetchAccountsForDomain(apiKey, domain);
    for (const email of targetEmails) {
      const account = accounts.get(email);
      if (!account) {
        missing.push(email);
        continue;
      }
      seen += 1;
      if (settingsOk(account)) {
        alreadyOk += 1;
      } else {
        toPatch.push(email);
      }
    }
    if ((seen + missing.length) % 990 === 0) {
      console.log(JSON.stringify({ stage: "scan", seen, missing: missing.length, toPatch: toPatch.length, alreadyOk }));
    }
  }

  console.log(JSON.stringify({
    stage: "patch_start",
    activeDomains: domains.length,
    targetInboxes: inboxes.length,
    seen,
    missing: missing.length,
    alreadyOk,
    toPatch: toPatch.length,
    concurrency: CONCURRENCY,
  }));

  let patched = 0;
  const failures = [];
  await forEachWithConcurrency(toPatch, CONCURRENCY, async (email) => {
    try {
      await patchAccount(apiKey, email);
      patched += 1;
      if (patched % 500 === 0) {
        console.log(JSON.stringify({ stage: "patch_progress", patched, remaining: toPatch.length - patched, failures: failures.length }));
      }
    } catch (err) {
      failures.push({ email, error: String(err?.message || err).slice(0, 300) });
    }
  });

  await sleep(5000);

  let okAfter = 0;
  let badAfter = 0;
  const badSamples = [];
  for (const [domain, targetEmails] of targetEmailsByDomain.entries()) {
    const accounts = await fetchAccountsForDomain(apiKey, domain);
    for (const email of targetEmails) {
      const account = accounts.get(email);
      if (account && settingsOk(account)) {
        okAfter += 1;
      } else {
        badAfter += 1;
        if (badSamples.length < 20) {
          badSamples.push({
            email,
            daily_limit: account?.daily_limit,
            sending_gap: account?.sending_gap,
            warmup_limit: account?.warmup?.limit,
            increment: account?.warmup?.increment,
            reply_rate: account?.warmup?.reply_rate,
          });
        }
      }
    }
  }

  console.log(JSON.stringify({
    stage: "done",
    activeDomains: domains.length,
    targetInboxes: inboxes.length,
    seen,
    missing: missing.length,
    alreadyOk,
    attemptedPatch: toPatch.length,
    patched,
    failures: failures.length,
    failureSamples: failures.slice(0, 10),
    okAfter,
    badAfter,
    badSamples,
  }, null, 2));

  if (missing.length > 0 || failures.length > 0 || badAfter > 0) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(JSON.stringify({ stage: "fatal", error: String(err?.message || err) }, null, 2));
  process.exitCode = 1;
});
