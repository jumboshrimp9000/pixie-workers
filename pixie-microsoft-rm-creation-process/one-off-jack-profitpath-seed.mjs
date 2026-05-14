import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import dotenv from "dotenv";
import axios from "axios";
import { createClient } from "@supabase/supabase-js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

dotenv.config({ path: path.resolve(__dirname, ".env"), override: false });
dotenv.config({ path: path.resolve(__dirname, "../../AP/.env"), override: false });

const args = new Set(process.argv.slice(2).filter((arg) => !arg.includes("=")));
const argValue = (name, fallback = "") => {
  const prefix = `${name}=`;
  const found = process.argv.slice(2).find((arg) => arg.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
};

const LIVE = args.has("--live");
const LIMIT = Number(argValue("--limit", "0")) || 0;
const START = Math.max(0, Number(argValue("--start", "0")) || 0);
const PHASE = argValue("--phase", "all"); // all | seed | dns
const CONCURRENCY = Math.max(1, Number(argValue("--concurrency", "10")) || 10);
const WAIT_CF_ACTIVE_SECONDS = Math.max(
  0,
  Number(argValue("--wait-cf-active-seconds", LIVE ? "900" : "0")) || 0
);

const OLD_CSV = argValue(
  "--old",
  "/Users/omermullick/Downloads/mailboxpro 242 domains to replace 5-8-2026 (2).csv"
);
const NEW_CSV = argValue(
  "--new",
  "/Users/omermullick/Downloads/242 Domains - Sheet1.csv"
);

const CUSTOMER_EMAIL = "jack@profitpath.com";
const CUSTOMER_FIRST = "Jack";
const CUSTOMER_LAST = "ProfitPath";
const WORKSPACE_EXTERNAL_ID = "profitpath";
const WORKSPACE_NAME = "ProfitPath";
const BATCH_ORDER_ID = "JACK-PROFITPATH-242-2026-05-10";
const INSTANTLY_CREDENTIAL_NAME = "ProfitPath Instantly";
const DEFAULT_MAILBOX_TARGET = 99;

const AIRTABLE_BASE_ID = "appWBfYieTo5Q2UzI";
const AIRTABLE_MS_DOMAINS_TABLE = "tblaSskAEX9s9YJa9";
const AIRTABLE_MS_ADMINS_TABLE = "tblDQBeLybmshAdw0";
const AIRTABLE_VIEW = "viwcu9aA4Yv2XpK2o";

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY || "";
const INSTANTLY_API_KEY = process.env.INSTANTLY_API_KEY || "";
const PORKBUN_API_KEY = process.env.PORKBUN_API_KEY || "";
const PORKBUN_SECRET_API_KEY = process.env.PORKBUN_SECRET_API_KEY || "";
const CLOUDFLARE_API_TOKEN = process.env.CLOUDFLARE_API_TOKEN || "";
const CLOUDFLARE_GLOBAL_KEY = process.env.CLOUDFLARE_GLOBAL_KEY || "";
const CLOUDFLARE_EMAIL = process.env.CLOUDFLARE_EMAIL || "";
const CLOUDFLARE_ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID || "";

const CF_API = "https://api.cloudflare.com/client/v4";
const PORKBUN_API = "https://api.porkbun.com/api/json/v3";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) fail("Missing Supabase service-role environment.");
if (!AIRTABLE_API_KEY) fail("Missing AIRTABLE_API_KEY.");
if (!INSTANTLY_API_KEY) fail("Missing INSTANTLY_API_KEY.");
if ((PHASE === "all" || PHASE === "dns") && (!PORKBUN_API_KEY || !PORKBUN_SECRET_API_KEY)) {
  fail("Missing PORKBUN_API_KEY/PORKBUN_SECRET_API_KEY.");
}
if ((PHASE === "all" || PHASE === "dns") && (!CLOUDFLARE_ACCOUNT_ID || (!CLOUDFLARE_API_TOKEN && !(CLOUDFLARE_GLOBAL_KEY && CLOUDFLARE_EMAIL)))) {
  fail("Missing Cloudflare environment.");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
});

function fail(message) {
  console.error(`one-off seed failed: ${message}`);
  process.exit(1);
}

function log(message) {
  const mode = LIVE ? "LIVE" : "DRY";
  console.log(`[${mode}] ${message}`);
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let quoted = false;

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (quoted) {
      if (ch === "\"") {
        if (text[i + 1] === "\"") {
          field += "\"";
          i += 1;
        } else {
          quoted = false;
        }
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === "\"") quoted = true;
    else if (ch === ",") {
      row.push(field);
      field = "";
    } else if (ch === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
    } else if (ch !== "\r") {
      field += ch;
    }
  }

  if (field.length || row.length) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}

function normalizeDomain(value) {
  return String(value || "").trim().toLowerCase().replace(/^https?:\/\//, "").replace(/\/.*$/, "");
}

function slugify(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");
}

function usernameFromName(firstName, fallback, used) {
  const base =
    String(firstName || fallback || "inbox")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9._-]+/g, "")
      .replace(/^[._-]+|[._-]+$/g, "") || "inbox";
  let candidate = base;
  let suffix = 2;
  while (used.has(candidate)) {
    candidate = `${base}${suffix}`;
    suffix += 1;
  }
  used.add(candidate);
  return candidate;
}

function parseJsonArray(raw, label, domain) {
  if (!raw) return [];
  try {
    const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    throw new Error(`Could not parse ${label} JSON for ${domain}: ${error.message}`);
  }
}

async function airtableList(tableId, params = {}) {
  const output = [];
  let offset = "";
  do {
    const query = new URLSearchParams({ pageSize: "100", ...params });
    if (offset) query.set("offset", offset);
    const response = await axios.get(
      `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${tableId}?${query}`,
      { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }, timeout: 30000 }
    );
    output.push(...(response.data?.records || []));
    offset = response.data?.offset || "";
  } while (offset);
  return output;
}

function cfHeaders() {
  if (CLOUDFLARE_GLOBAL_KEY && CLOUDFLARE_EMAIL) {
    return {
      "X-Auth-Key": CLOUDFLARE_GLOBAL_KEY,
      "X-Auth-Email": CLOUDFLARE_EMAIL,
      "Content-Type": "application/json",
    };
  }
  return { Authorization: `Bearer ${CLOUDFLARE_API_TOKEN}`, "Content-Type": "application/json" };
}

async function withRetry(label, fn, attempts = 5) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      const status = error.response?.status || 0;
      const retryable = status === 429 || status >= 500 || error.code === "ECONNRESET" || error.code === "ETIMEDOUT";
      if (!retryable || attempt === attempts) break;
      const retryAfter = Number(error.response?.headers?.["retry-after"] || "0") || 0;
      const delayMs = retryAfter > 0
        ? retryAfter * 1000
        : status === 429
          ? attempt * 15000
          : attempt * 5000;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  throw new Error(`${label} failed after ${attempts} attempts: ${lastError?.message || "unknown error"}`);
}

async function getOrCreateCloudflareZone(domain) {
  const headers = cfHeaders();
  const existing = await withRetry(`Cloudflare zone lookup for ${domain}`, () =>
    axios.get(`${CF_API}/zones`, {
      headers,
      params: { name: domain },
      timeout: 20000,
    })
  );
  if (existing.data?.result?.[0]?.id) {
    return {
      zoneId: existing.data.result[0].id,
      nameservers: existing.data.result[0].name_servers || [],
      created: false,
      status: existing.data.result[0].status || "",
    };
  }

  if (!LIVE) return { zoneId: "", nameservers: [], created: true, status: "dry-run" };

  const created = await withRetry(`Cloudflare zone create for ${domain}`, () =>
    axios.post(
      `${CF_API}/zones`,
      { name: domain, account: { id: CLOUDFLARE_ACCOUNT_ID }, type: "full" },
      { headers, timeout: 30000 }
    )
  );
  return {
    zoneId: created.data?.result?.id || "",
    nameservers: created.data?.result?.name_servers || [],
    created: true,
    status: created.data?.result?.status || "",
  };
}

async function getCloudflareZone(zoneId) {
  const response = await withRetry(`Cloudflare zone status for ${zoneId}`, () =>
    axios.get(`${CF_API}/zones/${zoneId}`, {
      headers: cfHeaders(),
      timeout: 20000,
    })
  );
  return response.data?.result || null;
}

async function waitForCloudflareActive(zoneId, domain) {
  if (!WAIT_CF_ACTIVE_SECONDS) {
    try {
      const zone = await getCloudflareZone(zoneId);
      return { active: zone?.status === "active", status: zone?.status || "" };
    } catch (error) {
      return { active: false, status: "", error: `${domain} Cloudflare status check failed: ${error.message}` };
    }
  }

  const deadline = Date.now() + WAIT_CF_ACTIVE_SECONDS * 1000;
  let status = "";
  let lastError = "";
  while (Date.now() <= deadline) {
    try {
      const zone = await getCloudflareZone(zoneId);
      status = zone?.status || "";
      lastError = "";
      if (status === "active") return { active: true, status };
    } catch (error) {
      lastError = error.message;
    }
    await new Promise((resolve) => setTimeout(resolve, 20000));
  }
  return {
    active: false,
    status,
    error: lastError
      ? `${domain} Cloudflare status check still failing after ${WAIT_CF_ACTIVE_SECONDS}s: ${lastError}`
      : `${domain} Cloudflare zone still ${status || "unknown"} after ${WAIT_CF_ACTIVE_SECONDS}s`,
  };
}

async function porkbunGetNs(domain) {
  const response = await withRetry(`Porkbun getNs for ${domain}`, () =>
    axios.post(
      `${PORKBUN_API}/domain/getNs/${domain}`,
      { apikey: PORKBUN_API_KEY, secretapikey: PORKBUN_SECRET_API_KEY },
      { timeout: 30000 }
    )
  );
  return response.data?.ns || [];
}

async function porkbunUpdateNs(domain, nameservers) {
  if (!LIVE) return { updated: true, dryRun: true };
  const response = await withRetry(`Porkbun updateNs for ${domain}`, () =>
    axios.post(
      `${PORKBUN_API}/domain/updateNs/${domain}`,
      { apikey: PORKBUN_API_KEY, secretapikey: PORKBUN_SECRET_API_KEY, ns: nameservers },
      { timeout: 30000 }
    )
  );
  if (response.data?.status !== "SUCCESS") {
    throw new Error(response.data?.message || `Porkbun updateNs returned ${response.data?.status || "unknown"}`);
  }
  return { updated: true };
}

async function validateInstantly() {
  const workspace = await axios.get("https://api.instantly.ai/api/v2/workspaces/current", {
    headers: { Authorization: `Bearer ${INSTANTLY_API_KEY}` },
    timeout: 20000,
  });
  const oauth = await axios.post(
    "https://api.instantly.ai/api/v2/oauth/microsoft/init",
    {},
    { headers: { Authorization: `Bearer ${INSTANTLY_API_KEY}` }, timeout: 20000 }
  );
  if (!oauth.data?.auth_url || !oauth.data?.session_id) {
    throw new Error("Instantly Microsoft OAuth init did not return auth_url/session_id.");
  }
  return workspace.data?.name || workspace.data?.id || "current workspace";
}

async function findOrInsert(table, lookup, row) {
  const query = supabase.from(table).select("*");
  Object.entries(lookup).forEach(([key, value]) => query.eq(key, value));
  const { data: existing, error: lookupError } = await query.maybeSingle();
  if (lookupError) throw new Error(`Lookup ${table} failed: ${lookupError.message}`);
  if (existing) return { row: existing, created: false };

  if (!LIVE) return { row: { id: randomUUID(), ...row }, created: true };
  const { data, error } = await supabase.from(table).insert(row).select("*").single();
  if (error) throw new Error(`Insert ${table} failed: ${error.message}`);
  return { row: data, created: true };
}

async function insertIfMissing(table, lookup, row) {
  const result = await findOrInsert(table, lookup, row);
  return result;
}

async function ensureCustomerAndWorkspace() {
  const { row: customer, created: customerCreated } = await findOrInsert(
    "customers",
    { email: CUSTOMER_EMAIL },
    {
      email: CUSTOMER_EMAIL,
      first_name: CUSTOMER_FIRST,
      last_name: CUSTOMER_LAST,
      credit_balance: 0,
      agency_mode: false,
      workspace_wallets_enabled: true,
      onboarding_completed: true,
    }
  );

  const { row: workspace, created: workspaceCreated } = await findOrInsert(
    "workspaces",
    { customer_id: customer.id, external_id: WORKSPACE_EXTERNAL_ID },
    {
      customer_id: customer.id,
      external_id: WORKSPACE_EXTERNAL_ID,
      name: WORKSPACE_NAME,
      slug: WORKSPACE_EXTERNAL_ID,
      is_default: true,
      archived: false,
      status: "active",
      wallet_balance: 0,
      wallet_mode: "use-master",
      fallback_to_master: true,
      use_master_billing: true,
    }
  );

  return { customer, workspace, customerCreated, workspaceCreated };
}

async function ensureSendingToolCredential(customer, workspace) {
  const { row: tool } = await findOrInsert(
    "sending_tools",
    { slug: "instantly" },
    { slug: "instantly", name: "Instantly.ai", has_api: true, extra_field_schema: {} }
  );

  const { row: credential, created } = await findOrInsert(
    "sending_tool_credentials",
    { customer_id: customer.id, name: INSTANTLY_CREDENTIAL_NAME },
    {
      customer_id: customer.id,
      sending_tool_id: tool.id,
      name: INSTANTLY_CREDENTIAL_NAME,
      username: "support@mailboxpro.io",
      password: "",
      workspace_name: WORKSPACE_NAME,
      api_key: INSTANTLY_API_KEY,
      tool_url: "https://app.instantly.ai",
      extra_fields: {
        scope_type: "client",
        scope_key: WORKSPACE_EXTERNAL_ID,
        source: "jack_profitpath_242_migration",
        settings: {
          enableWarmup: true,
          instantlyWarmup: { enabled: true },
        },
      },
      is_default: true,
    }
  );

  if (LIVE && !created) {
    const { error } = await supabase
      .from("sending_tool_credentials")
      .update({
        sending_tool_id: tool.id,
        api_key: INSTANTLY_API_KEY,
        workspace_name: WORKSPACE_NAME,
        username: "support@mailboxpro.io",
        extra_fields: {
          ...(credential.extra_fields || {}),
          scope_type: "client",
          scope_key: WORKSPACE_EXTERNAL_ID,
          source: "jack_profitpath_242_migration",
        },
      })
      .eq("id", credential.id);
    if (error) throw new Error(`Update Instantly credential failed: ${error.message}`);
  }

  return { tool, credential, credentialCreated: created };
}

async function ensureBatch(customer, workspace, totalInboxes) {
  const { data: batches, error } = await supabase
    .from("order_batches")
    .select("*")
    .eq("customer_id", customer.id)
    .contains("order_ids", [BATCH_ORDER_ID])
    .limit(1);
  if (error) throw new Error(`Lookup order batch failed: ${error.message}`);
  if (batches?.[0]) return { batch: batches[0], created: false };

  const row = {
    customer_id: customer.id,
    workspace_id: workspace.id,
    provider: "azure",
    total_inboxes: totalInboxes,
    total_cost: 0,
    domain_purchase_cost: 0,
    charge_source: "global",
    status: "processing",
    csv_mode: true,
    order_ids: [BATCH_ORDER_ID],
  };
  if (!LIVE) return { batch: { id: randomUUID(), ...row }, created: true };
  const { data, error: insertError } = await supabase.from("order_batches").insert(row).select("*").single();
  if (insertError) throw new Error(`Insert order batch failed: ${insertError.message}`);
  return { batch: data, created: true };
}

async function ensureAdminCredential(airtableAdmin) {
  const fields = airtableAdmin.fields || {};
  const email = String(fields["Admin Email"] || "").trim().toLowerCase();
  const password = String(fields["Admin Password"] || "").trim();
  if (!email || !password) throw new Error(`Airtable admin ${airtableAdmin.id} missing email/password`);

  const { data: existing, error } = await supabase
    .from("admin_credentials")
    .select("*")
    .eq("provider", "microsoft")
    .eq("email", email)
    .maybeSingle();
  if (error) throw new Error(`Lookup admin credential failed for ${email}: ${error.message}`);
  if (existing) return { admin: existing, created: false };

  const row = {
    provider: "microsoft",
    email,
    password,
    extra_fields: {
      source: "airtable_jack_profitpath_242_migration",
      airtable_record_id: airtableAdmin.id,
    },
    usage_count: 0,
    active: true,
  };
  if (!LIVE) return { admin: { id: randomUUID(), ...row }, created: true };
  const { data, error: insertError } = await supabase.from("admin_credentials").insert(row).select("*").single();
  if (insertError) throw new Error(`Insert admin credential failed for ${email}: ${insertError.message}`);
  return { admin: data, created: true };
}

function buildInboxes(oldFields, oldDomain, newDomain) {
  const target = Number(oldFields["Mailboxes Target"] || DEFAULT_MAILBOX_TARGET) || DEFAULT_MAILBOX_TARGET;
  const names = parseJsonArray(oldFields["First Last Name JSON"], "First Last Name", oldDomain);
  const oldCreated = parseJsonArray(oldFields["Created User JSON"], "Created User", oldDomain);
  const password = String(oldFields["Inbox Password"] || "").trim();
  if (!password) throw new Error(`Missing inbox password on old domain ${oldDomain}`);

  const used = new Set();
  const output = [];
  for (let index = 0; index < target; index += 1) {
    const created = oldCreated[index] || {};
    const name = names[index] || {};
    const firstName = String(created.FirstName || created.firstName || name.FirstName || name.firstName || `Inbox${index + 1}`).trim();
    const lastName = String(created.LastName || created.lastName || name.LastName || name.lastName || "").trim();
    const oldUsername = String(created.Username || created.email || "").split("@")[0].trim().toLowerCase();
    const username = oldUsername && !used.has(oldUsername)
      ? (used.add(oldUsername), oldUsername)
      : usernameFromName(firstName, `inbox${index + 1}`, used);
    output.push({
      username,
      first_name: firstName,
      last_name: lastName,
      email: `${username}@${newDomain}`,
      password,
      status: "pending",
      billing_type: "paid",
      is_admin: index === 0,
    });
  }
  return output;
}

async function seedDomain(mapping, context) {
  const { customer, workspace, batch, credential, adminByAirtableId, zoneByDomain } = context;
  const oldFields = mapping.oldRecord.fields || {};
  const adminRecord = adminByAirtableId.get(mapping.adminId);
  if (!adminRecord) throw new Error(`No Supabase admin for Airtable admin ${mapping.adminId}`);

  const existingDomain = await supabase
    .from("domains")
    .select("*")
    .eq("domain", mapping.newDomain)
    .maybeSingle();
  if (existingDomain.error) throw new Error(`Domain lookup failed for ${mapping.newDomain}: ${existingDomain.error.message}`);

  let domain = existingDomain.data;
  let domainCreated = false;
  if (!domain) {
    const zoneInfo = zoneByDomain.get(mapping.newDomain) || {};
    const domainRow = {
      order_batch_id: batch.id,
      customer_id: customer.id,
      workspace_id: workspace.id,
      domain: mapping.newDomain,
      provider: "azure",
      source: "own",
      status: "pending",
      interim_status: zoneInfo.zoneId ? "Both - DNS Zone Created" : "Both - New Order",
      action_history: `One-off ProfitPath replacement seeded from ${mapping.oldDomain}`,
      nameservers_moved: Boolean(zoneInfo.nameserversMoved),
      cloudflare_zone_id: zoneInfo.zoneId || null,
      payment_status: "paid",
      price_per_inbox: 0,
      monthly_cost: 0,
      fulfillment_settings: {
        sending_tool: "instantly",
        instantly_workspace: WORKSPACE_NAME,
        replacement_old_domain: mapping.oldDomain,
        airtable_old_record_id: mapping.oldRecord.id,
      },
    };
    if (!LIVE) {
      domain = { id: randomUUID(), ...domainRow };
      domainCreated = true;
    } else {
      const inserted = await supabase.from("domains").insert(domainRow).select("*").single();
      if (inserted.error) throw new Error(`Insert domain failed for ${mapping.newDomain}: ${inserted.error.message}`);
      domain = inserted.data;
      domainCreated = true;
    }
  } else if (domain.customer_id !== customer.id) {
    throw new Error(`${mapping.newDomain} already exists under a different customer (${domain.customer_id})`);
  }

  const inboxRows = buildInboxes(oldFields, mapping.oldDomain, mapping.newDomain).map((inbox) => ({
    ...inbox,
    domain_id: domain.id,
    customer_id: customer.id,
  }));

  let insertedInboxes = 0;
  if (LIVE) {
    const { data: existingInboxes, error: inboxLookupError } = await supabase
      .from("inboxes")
      .select("email")
      .eq("domain_id", domain.id);
    if (inboxLookupError) throw new Error(`Lookup inboxes failed for ${mapping.newDomain}: ${inboxLookupError.message}`);
    const existingEmails = new Set((existingInboxes || []).map((row) => String(row.email || "").toLowerCase()));
    const toInsert = inboxRows.filter((row) => !existingEmails.has(row.email.toLowerCase()));
    if (toInsert.length > 0) {
      const { error: insertInboxError } = await supabase.from("inboxes").insert(toInsert);
      if (insertInboxError) throw new Error(`Insert inboxes failed for ${mapping.newDomain}: ${insertInboxError.message}`);
    }
    insertedInboxes = toInsert.length;
  } else {
    insertedInboxes = inboxRows.length;
  }

  await insertIfMissing(
    "domain_admin_assignments",
    { domain_id: domain.id, admin_cred_id: adminRecord.id },
    { domain_id: domain.id, admin_cred_id: adminRecord.id }
  );

  await insertIfMissing(
    "domain_credentials",
    { domain_id: domain.id, credential_id: credential.id },
    { domain_id: domain.id, credential_id: credential.id }
  );

  await insertIfMissing(
    "actions",
    { domain_id: domain.id, type: "provision_inbox" },
    {
      type: "provision_inbox",
      status: "pending",
      domain_id: domain.id,
      customer_id: customer.id,
      order_batch_id: batch.id,
      max_attempts: 8,
      payload: {
        domain: mapping.newDomain,
        provider: "azure",
        source: "own",
        inbox_count: DEFAULT_MAILBOX_TARGET,
        one_off: "jack_profitpath_242",
        old_domain: mapping.oldDomain,
        airtable_old_record_id: mapping.oldRecord.id,
      },
    }
  );

  return { domainCreated, insertedInboxes };
}

async function runDnsPhase(mappings) {
  const zoneByDomain = new Map();
  const failures = [];

  await mapLimit(mappings, CONCURRENCY, async (mapping) => {
    try {
      const zone = await getOrCreateCloudflareZone(mapping.newDomain);
      if (!LIVE && !zone.zoneId) {
        zoneByDomain.set(mapping.newDomain, {
          zoneId: "",
          nameservers: [],
          nameserversMoved: false,
          alreadyMoved: false,
          zoneCreated: true,
          dryRun: true,
        });
        return;
      }
      let currentNs = [];
      if (zone.zoneId && zone.nameservers.length >= 2) {
        currentNs = await porkbunGetNs(mapping.newDomain);
        const wanted = new Set(zone.nameservers.map((value) => value.toLowerCase()));
        const alreadyMoved =
          currentNs.length >= 2 &&
          currentNs.every((value) => wanted.has(String(value).toLowerCase()));
        if (!alreadyMoved) await porkbunUpdateNs(mapping.newDomain, zone.nameservers);
        const activeState = await waitForCloudflareActive(zone.zoneId, mapping.newDomain);
        if (LIVE && WAIT_CF_ACTIVE_SECONDS > 0 && !activeState.active) {
          throw new Error(activeState.error || `Cloudflare zone status is ${activeState.status || "unknown"}`);
        }
        zoneByDomain.set(mapping.newDomain, {
          zoneId: zone.zoneId,
          nameservers: zone.nameservers,
          nameserversMoved: true,
          alreadyMoved,
          zoneCreated: zone.created,
          cloudflareStatus: activeState.status,
        });
      } else {
        throw new Error("Cloudflare did not return at least two nameservers");
      }
    } catch (error) {
      failures.push({ domain: mapping.newDomain, error: error.message });
    }
  });

  return { zoneByDomain, failures };
}

async function mapLimit(items, concurrency, fn) {
  let cursor = 0;
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;
      await fn(items[index], index);
    }
  });
  await Promise.all(workers);
}

async function main() {
  log(`Mode: ${LIVE ? "LIVE WRITES ENABLED" : "dry-run only"}; phase=${PHASE}; concurrency=${CONCURRENCY}; cfActiveWait=${WAIT_CF_ACTIVE_SECONDS}s`);

  const instantlyWorkspace = await validateInstantly();
  log(`Instantly credential validated for workspace: ${instantlyWorkspace}`);

  const oldDomains = parseCsv(fs.readFileSync(OLD_CSV, "utf8"))
    .map((row) => normalizeDomain(row[0]))
    .filter(Boolean);
  const newDomains = parseCsv(fs.readFileSync(NEW_CSV, "utf8"))
    .map((row) => normalizeDomain(row[0]))
    .filter(Boolean);

  if (oldDomains.length !== newDomains.length) fail(`CSV count mismatch: old=${oldDomains.length}, new=${newDomains.length}`);
  if (new Set(oldDomains).size !== oldDomains.length) fail("Old domain CSV has duplicates.");
  if (new Set(newDomains).size !== newDomains.length) fail("New domain CSV has duplicates.");

  const slicedOld = oldDomains.slice(START, LIMIT ? START + LIMIT : undefined);
  const slicedNew = newDomains.slice(START, LIMIT ? START + LIMIT : undefined);

  const airtableDomains = await airtableList(AIRTABLE_MS_DOMAINS_TABLE, { view: AIRTABLE_VIEW });
  const airtableAdmins = await airtableList(AIRTABLE_MS_ADMINS_TABLE);
  const oldByDomain = new Map(airtableDomains.map((record) => [normalizeDomain(record.fields?.Domain), record]));
  const adminById = new Map(airtableAdmins.map((record) => [record.id, record]));

  const mappings = slicedOld.map((oldDomain, offset) => {
    const newDomain = slicedNew[offset];
    const oldRecord = oldByDomain.get(oldDomain);
    if (!oldRecord) throw new Error(`Old domain not found in Airtable view: ${oldDomain}`);
    const adminId = oldRecord.fields?.["MS Admin"]?.[0];
    if (!adminId || !adminById.has(adminId)) throw new Error(`Old domain ${oldDomain} missing linked MS Admin`);
    const inboxes = buildInboxes(oldRecord.fields || {}, oldDomain, newDomain);
    if (inboxes.length !== DEFAULT_MAILBOX_TARGET) throw new Error(`Expected 99 inboxes for ${oldDomain}, got ${inboxes.length}`);
    return {
      index: START + offset + 1,
      oldDomain,
      newDomain,
      oldRecord,
      adminId,
      airtableAdmin: adminById.get(adminId),
    };
  });

  log(`Mappings ready: ${mappings.length}/${oldDomains.length}`);
  log(`Unique Airtable admins in scope: ${new Set(mappings.map((row) => row.adminId)).size}`);
  log(`Total inboxes to create in scope: ${mappings.length * DEFAULT_MAILBOX_TARGET}`);

  let dnsResult = { zoneByDomain: new Map(), failures: [] };
  if (PHASE === "all" || PHASE === "dns") {
    dnsResult = await runDnsPhase(mappings);
    log(`DNS phase: zones prepared=${dnsResult.zoneByDomain.size}, failures=${dnsResult.failures.length}`);
    if (dnsResult.failures.length > 0) {
      console.log(JSON.stringify(dnsResult.failures.slice(0, 20), null, 2));
      if (LIVE) fail("DNS failures encountered; stopping before seed phase.");
    }
  }

  if (PHASE === "dns") {
    log("DNS-only phase complete.");
    return;
  }

  const { customer, workspace, customerCreated, workspaceCreated } = await ensureCustomerAndWorkspace();
  log(`Customer ${customerCreated ? "would be/was created" : "exists"}: ${CUSTOMER_EMAIL}`);
  log(`Workspace ${workspaceCreated ? "would be/was created" : "exists"}: ${WORKSPACE_NAME}`);

  const { credential, credentialCreated } = await ensureSendingToolCredential(customer, workspace);
  log(`Instantly credential ${credentialCreated ? "would be/was created" : "exists"}: ${INSTANTLY_CREDENTIAL_NAME}`);

  const { batch, created: batchCreated } = await ensureBatch(customer, workspace, mappings.length * DEFAULT_MAILBOX_TARGET);
  log(`Order batch ${batchCreated ? "would be/was created" : "exists"}: ${BATCH_ORDER_ID}`);

  const adminByAirtableId = new Map();
  let createdAdmins = 0;
  for (const adminId of new Set(mappings.map((row) => row.adminId))) {
    const { admin, created } = await ensureAdminCredential(adminById.get(adminId));
    adminByAirtableId.set(adminId, admin);
    if (created) createdAdmins += 1;
  }
  log(`Microsoft admin credentials ${LIVE ? "inserted" : "to insert if live"}: ${createdAdmins}`);

  let createdDomains = 0;
  let insertedInboxes = 0;
  for (const mapping of mappings) {
    const result = await seedDomain(mapping, {
      customer,
      workspace,
      batch,
      credential,
      adminByAirtableId,
      zoneByDomain: dnsResult.zoneByDomain,
    });
    if (result.domainCreated) createdDomains += 1;
    insertedInboxes += result.insertedInboxes;
  }

  log(`Domains ${LIVE ? "created" : "to create if live"}: ${createdDomains}`);
  log(`Inboxes ${LIVE ? "inserted" : "to insert if live"}: ${insertedInboxes}`);
  log(`Actions ${LIVE ? "ensured" : "to ensure if live"}: ${mappings.length}`);
  log("Complete.");
}

main().catch((error) => {
  fail(error.stack || error.message);
});
