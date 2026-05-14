#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import process from "node:process";

const AZURE_CLI_PUBLIC_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46";
const AIRTABLE_BASE_ID = "appIL8u7KDbKxV4dn";
const ORDERS_TABLE_ID = "tblMZrawskbbNSoM9";
const PANEL_TABLE_ID = "tblK16pTWj5W6BJJE";

const args = process.argv.slice(2);
const flags = new Set(args.filter((arg) => !arg.includes("=")));
const argValue = (name, fallback = "") => {
  const prefix = `${name}=`;
  const found = args.find((arg) => arg.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
};

const INPUT = argValue("--input", "");
const OUTPUT_DIR = argValue("--output-dir", path.resolve(process.cwd(), "logs/active-tenant-panel-mapping"));
const LIVE = flags.has("--live");
const LIMIT = Number(argValue("--limit", "0")) || 0;
const CONCURRENCY = Math.max(1, Number(argValue("--concurrency", "6")) || 6);
const EXPECTED_CREATE_COUNT = Number(argValue("--confirm-create-ready-count", "-1"));

const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY || "";

if (!INPUT) fail("Missing --input=/path/to/admins.csv");
if (!AIRTABLE_API_KEY) fail("Missing AIRTABLE_API_KEY");
if (LIVE && EXPECTED_CREATE_COUNT < 0) {
  fail("Live mode requires --confirm-create-ready-count=<exact ready_create count>");
}

fs.mkdirSync(OUTPUT_DIR, { recursive: true });

const outputJson = path.join(OUTPUT_DIR, "mapping.json");
const outputCsv = path.join(OUTPUT_DIR, "mapping.csv");
const outputSummary = path.join(OUTPUT_DIR, "summary.json");

function fail(message) {
  console.error(`active tenant panel mapping failed: ${message}`);
  process.exit(1);
}

function normalizeDomain(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/.*$/, "");
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\n\r]/.test(text)) return `"${text.replaceAll("\"", "\"\"")}"`;
  return text;
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

function readAdmins(filePath) {
  const rows = parseCsv(fs.readFileSync(filePath, "utf8"));
  if (!rows.length) fail(`Input CSV is empty: ${filePath}`);
  const headers = rows.shift().map((h) => String(h || "").trim().toLowerCase());
  const emailIndex = headers.findIndex((h) => ["email", "adminemail", "admin email"].includes(h));
  const passwordIndex = headers.findIndex((h) => ["password", "adminpassword", "admin password"].includes(h));
  if (emailIndex < 0 || passwordIndex < 0) {
    fail(`Input CSV must include email/password columns. Found: ${headers.join(", ")}`);
  }

  const seen = new Set();
  const admins = [];
  for (const row of rows) {
    const email = normalizeEmail(row[emailIndex]);
    const password = String(row[passwordIndex] || "");
    if (!email) continue;
    if (seen.has(email)) continue;
    seen.add(email);
    admins.push({ email, password });
  }
  return LIMIT > 0 ? admins.slice(0, LIMIT) : admins;
}

async function withRetry(label, fn, attempts = 4) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      const waitMs = Math.min(10_000, 600 * 2 ** (attempt - 1));
      if (attempt < attempts) await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }
  throw new Error(`${label}: ${lastError?.message || lastError}`);
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const text = await response.text();
  let body = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text;
  }
  if (!response.ok) {
    const message = typeof body === "string" ? body : JSON.stringify(body);
    throw new Error(`${response.status} ${response.statusText}: ${message}`);
  }
  return body;
}

async function getTenantId(tenantDomain) {
  const urls = [
    `https://login.microsoftonline.com/${tenantDomain}/v2.0/.well-known/openid-configuration`,
    `https://login.microsoftonline.com/${tenantDomain}/.well-known/openid-configuration`,
  ];
  let lastError = "";
  for (const url of urls) {
    try {
      const body = await fetchJson(url, { signal: AbortSignal.timeout(30_000) });
      const text = `${body.token_endpoint || ""}\n${body.issuer || ""}`;
      const match = text.match(/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/);
      if (match) return match[0];
    } catch (error) {
      lastError = error.message;
    }
  }
  throw new Error(`tenant id discovery failed for ${tenantDomain}: ${lastError}`);
}

async function getGraphToken({ tenantId, email, password }) {
  const form = new URLSearchParams();
  form.set("grant_type", "password");
  form.set("client_id", AZURE_CLI_PUBLIC_CLIENT_ID);
  form.set("scope", "https://graph.microsoft.com/.default");
  form.set("username", email);
  form.set("password", password);

  const body = await fetchJson(`https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: form,
    signal: AbortSignal.timeout(45_000),
  });
  if (!body.access_token) throw new Error(`token response missing access_token for ${email}`);
  return body.access_token;
}

async function getMicrosoftDomains(admin) {
  const tenantDomain = admin.email.split("@")[1];
  const tenantId = await withRetry(`tenant id ${admin.email}`, () => getTenantId(tenantDomain), 2);
  const token = await withRetry(`graph token ${admin.email}`, () => getGraphToken({ tenantId, ...admin }), 2);
  const body = await withRetry(`graph domains ${admin.email}`, () =>
    fetchJson(
      "https://graph.microsoft.com/v1.0/domains?$select=id,isDefault,isInitial,isVerified,isRoot,isAdminManaged,supportedServices,state",
      { headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(45_000) },
    ),
  );
  const domains = Array.isArray(body.value) ? body.value : [];
  const customDomains = domains
    .map((domain) => ({
      id: normalizeDomain(domain.id),
      isDefault: Boolean(domain.isDefault),
      isInitial: Boolean(domain.isInitial),
      isVerified: Boolean(domain.isVerified),
      isRoot: Boolean(domain.isRoot),
      state: domain.state || null,
    }))
    .filter((domain) => domain.id && !domain.id.endsWith(".onmicrosoft.com"));
  return { tenantId, domains, customDomains };
}

async function airtableList(tableId, fields) {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    for (const field of fields) params.append("fields[]", field);
    if (offset) params.set("offset", offset);
    const body = await withRetry(`airtable list ${tableId}`, () =>
      fetchJson(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${tableId}?${params}`, {
        headers: { authorization: `Bearer ${AIRTABLE_API_KEY}` },
        signal: AbortSignal.timeout(45_000),
      }),
    );
    for (const record of body.records || []) records.push(record);
    offset = body.offset || "";
  } while (offset);
  return records;
}

async function airtableCreate(tableId, records) {
  const created = [];
  for (let i = 0; i < records.length; i += 10) {
    const batch = records.slice(i, i + 10);
    const body = await withRetry(`airtable create ${tableId}`, () =>
      fetchJson(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${tableId}`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${AIRTABLE_API_KEY}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ records: batch }),
        signal: AbortSignal.timeout(45_000),
      }),
    );
    for (const record of body.records || []) created.push(record);
  }
  return created;
}

async function mapConcurrent(items, worker, concurrency) {
  const results = new Array(items.length);
  let next = 0;
  async function run() {
    while (next < items.length) {
      const index = next++;
      results[index] = await worker(items[index], index);
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, run));
  return results;
}

function getField(record, field) {
  return record.fields ? record.fields[field] : undefined;
}

function summarizeStatuses(rows) {
  const counts = {};
  for (const row of rows) counts[row.status] = (counts[row.status] || 0) + 1;
  return Object.fromEntries(Object.entries(counts).sort((a, b) => a[0].localeCompare(b[0])));
}

const admins = readAdmins(INPUT);
console.log(`Loaded ${admins.length} admins from ${INPUT}`);
console.log("Loading Airtable orders and panels...");
const [orderRecords, panelRecords] = await Promise.all([
  airtableList(ORDERS_TABLE_ID, ["Domain 1", "Panel", "Record ID"]),
  airtableList(PANEL_TABLE_ID, ["Microsoft Email", "Orders", "ID", "Created", "Domain 1 (from Orders)"]),
]);

const ordersByDomain = new Map();
for (const record of orderRecords) {
  const domain = normalizeDomain(getField(record, "Domain 1"));
  if (!domain) continue;
  if (!ordersByDomain.has(domain)) ordersByDomain.set(domain, []);
  ordersByDomain.get(domain).push(record);
}

const panelsByAdmin = new Map();
const panelsByOrder = new Map();
let maxPanelId = 0;
for (const record of panelRecords) {
  const panelId = Number(getField(record, "ID")) || 0;
  if (panelId > maxPanelId) maxPanelId = panelId;
  const email = normalizeEmail(getField(record, "Microsoft Email"));
  if (email) {
    if (!panelsByAdmin.has(email)) panelsByAdmin.set(email, []);
    panelsByAdmin.get(email).push(record);
  }
  const orders = Array.isArray(getField(record, "Orders")) ? getField(record, "Orders") : [];
  for (const orderId of orders) {
    if (!panelsByOrder.has(orderId)) panelsByOrder.set(orderId, []);
    panelsByOrder.get(orderId).push(record);
  }
}

console.log(`Airtable loaded: orders=${orderRecords.length}, panels=${panelRecords.length}, current_max_panel_id=${maxPanelId}`);
console.log(`Checking Microsoft domains with concurrency=${CONCURRENCY}...`);

const rows = await mapConcurrent(
  admins,
  async (admin, index) => {
    const base = {
      index: index + 1,
      admin_email: admin.email,
      tenant_id: "",
      custom_domains: [],
      chosen_domain: "",
      order_record_ids: [],
      existing_panel_ids_by_admin: [],
      existing_panel_ids_by_order: [],
      status: "",
      error: "",
    };

    try {
      const ms = await getMicrosoftDomains(admin);
      base.tenant_id = ms.tenantId;
      base.custom_domains = ms.customDomains.map((domain) => domain.id);

      if (ms.customDomains.length === 0) {
        base.status = "no_custom_domain";
        return base;
      }
      if (ms.customDomains.length > 1) {
        base.status = "multiple_custom_domains_needs_review";
        return base;
      }

      const chosen = ms.customDomains[0].id;
      base.chosen_domain = chosen;
      const orders = ordersByDomain.get(chosen) || [];
      base.order_record_ids = orders.map((record) => record.id);
      base.existing_panel_ids_by_admin = (panelsByAdmin.get(admin.email) || []).map((record) => record.id);
      for (const order of orders) {
        for (const panel of panelsByOrder.get(order.id) || []) {
          base.existing_panel_ids_by_order.push(panel.id);
        }
      }
      base.existing_panel_ids_by_order = [...new Set(base.existing_panel_ids_by_order)];

      if (orders.length === 0) {
        base.status = "no_airtable_domain1_match";
      } else if (orders.length > 1) {
        base.status = "multiple_airtable_domain1_matches_needs_review";
      } else if (base.existing_panel_ids_by_admin.length > 0) {
        base.status = "existing_panel_for_admin_skip";
      } else if (base.existing_panel_ids_by_order.length > 0) {
        base.status = "existing_panel_for_order_skip";
      } else {
        base.status = "ready_create";
      }
      return base;
    } catch (error) {
      base.status = "microsoft_lookup_error";
      base.error = error.message || String(error);
      return base;
    }
  },
  CONCURRENCY,
);

const readyRows = rows.filter((row) => row.status === "ready_create");
const summary = {
  input: INPUT,
  live: LIVE,
  checked_at: new Date().toISOString(),
  admin_count: admins.length,
  airtable_order_count: orderRecords.length,
  airtable_panel_count: panelRecords.length,
  current_max_panel_id: maxPanelId,
  status_counts: summarizeStatuses(rows),
  ready_create_count: readyRows.length,
  output_json: outputJson,
  output_csv: outputCsv,
};

fs.writeFileSync(outputJson, `${JSON.stringify(rows, null, 2)}\n`);

const csvHeaders = [
  "index",
  "admin_email",
  "tenant_id",
  "custom_domains",
  "chosen_domain",
  "order_record_ids",
  "existing_panel_ids_by_admin",
  "existing_panel_ids_by_order",
  "status",
  "error",
];
fs.writeFileSync(
  outputCsv,
  [
    csvHeaders.join(","),
    ...rows.map((row) =>
      csvHeaders
        .map((key) => {
          const value = row[key];
          return csvEscape(Array.isArray(value) ? value.join(";") : value);
        })
        .join(","),
    ),
  ].join("\n") + "\n",
);

if (LIVE) {
  if (readyRows.length !== EXPECTED_CREATE_COUNT) {
    fail(`Refusing live create: ready_create_count=${readyRows.length}, expected=${EXPECTED_CREATE_COUNT}`);
  }
  const adminByEmail = new Map(admins.map((admin) => [admin.email, admin]));
  const createRecords = readyRows.map((row) => {
    const admin = adminByEmail.get(row.admin_email);
    return {
      fields: {
        "Microsoft Email": row.admin_email,
        "Admin Password (Manual)": admin.password,
        "Checkout Status": "Active",
        "License Provider": "Pax8",
        Orders: row.order_record_ids,
      },
    };
  });
  const created = await airtableCreate(PANEL_TABLE_ID, createRecords);
  summary.created_count = created.length;
  summary.created_record_ids = created.map((record) => record.id);
}

fs.writeFileSync(outputSummary, `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
