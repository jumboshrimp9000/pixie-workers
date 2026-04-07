/**
 * Part 1: Universal Domain Setup
 *
 * Handles domain-level infrastructure that applies to ALL providers (Microsoft + Google):
 *   1. Domain purchase via Dynadot (if source = 'buy')
 *   2. Cloudflare zone creation
 *   3. Nameserver migration to Cloudflare (purchased domains)
 *   4. NS propagation check (informational)
 *
 * This runs as a standalone TypeScript script called by the orchestrator (run.ps1).
 * It takes a domain_id as argument, processes that domain, and exits.
 *
 * Usage: npx tsx Part1-UniversalDomainSetup.ts <domain_id> <action_id>
 */

import { createClient } from "@supabase/supabase-js";
import axios from "axios";
import dotenv from "dotenv";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, ".env") });

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
const CF_TOKEN = process.env.CLOUDFLARE_API_TOKEN || "";
const CF_GLOBAL_KEY = process.env.CLOUDFLARE_GLOBAL_KEY || "";
const CF_EMAIL = process.env.CLOUDFLARE_EMAIL || "";
const CF_ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID || "";
const DYNADOT_KEY = process.env.DYNADOT_API_KEY || "";
const CF_API = "https://api.cloudflare.com/client/v4";
const DYNADOT_API = "https://api.dynadot.com/api3.json";

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
});

// ─────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────

function log(level: "info" | "ok" | "warn" | "err", msg: string) {
  const prefix = { info: "-->", ok: "[OK]", warn: "[WARN]", err: "[ERR]" };
  const ts = new Date().toISOString().replace("T", " ").slice(0, 19);
  console.log(`[${ts}] ${prefix[level]} ${msg}`);
}

async function updateDomain(domainId: string, fields: Record<string, any>) {
  await supabase.from("domains").update({ ...fields, updated_at: new Date().toISOString() }).eq("id", domainId);
}

async function addActionLog(
  actionId: string,
  domainId: string,
  customerId: string,
  eventType: string,
  severity: "info" | "warn" | "error",
  message: string,
  metadata?: Record<string, any>
) {
  await supabase.from("action_logs").insert({
    action_id: actionId,
    domain_id: domainId,
    customer_id: customerId,
    event_type: eventType,
    severity,
    message,
    metadata: metadata || {},
  });
}

// ─────────────────────────────────────────────────────────────────────
// Cloudflare
// ─────────────────────────────────────────────────────────────────────

function cfHeaders() {
  // Prefer Global API Key (has DNS:Edit), fall back to API Token
  if (CF_GLOBAL_KEY && CF_EMAIL) {
    return { "X-Auth-Key": CF_GLOBAL_KEY, "X-Auth-Email": CF_EMAIL, "Content-Type": "application/json" };
  }
  return { Authorization: `Bearer ${CF_TOKEN}`, "Content-Type": "application/json" };
}

async function getOrCreateCloudflareZone(domain: string): Promise<string | null> {
  if ((!CF_TOKEN && !CF_GLOBAL_KEY) || !CF_ACCOUNT_ID) return null;
  const headers = cfHeaders();

  // Check existing
  try {
    const resp = await axios.get(`${CF_API}/zones?name=${encodeURIComponent(domain)}`, { headers, timeout: 15000 });
    if (resp.data?.result?.length > 0) return resp.data.result[0].id;
  } catch {}

  // Create
  try {
    const resp = await axios.post(
      `${CF_API}/zones`,
      { name: domain, account: { id: CF_ACCOUNT_ID }, type: "full" },
      { headers, timeout: 30000 }
    );
    if (resp.data?.result?.id) return resp.data.result.id;
  } catch (err: any) {
    log("err", `Failed to create CF zone for ${domain}: ${err.message}`);
  }
  return null;
}

async function getCloudflareNameservers(zoneId: string): Promise<string[] | null> {
  if (!CF_TOKEN && !CF_GLOBAL_KEY) return null;
  const headers = cfHeaders();
  try {
    const resp = await axios.get(`${CF_API}/zones/${zoneId}`, { headers, timeout: 15000 });
    return resp.data?.result?.name_servers || null;
  } catch {
    return null;
  }
}

async function isCloudflareZoneActive(zoneId: string): Promise<boolean> {
  if (!CF_TOKEN && !CF_GLOBAL_KEY) return false;
  const headers = cfHeaders();
  try {
    const resp = await axios.get(`${CF_API}/zones/${zoneId}`, { headers, timeout: 15000 });
    return resp.data?.result?.status === "active";
  } catch {
    return false;
  }
}

// ─────────────────────────────────────────────────────────────────────
// Dynadot NS migration
// ─────────────────────────────────────────────────────────────────────

async function setDynadotNameservers(domain: string, ns0: string, ns1: string): Promise<boolean> {
  if (!DYNADOT_KEY) return false;
  try {
    const params = new URLSearchParams({ key: DYNADOT_KEY, command: "set_ns", domain, ns0, ns1 });
    const resp = await axios.get(`${DYNADOT_API}?${params}`, { timeout: 30000 });
    return resp.data?.SetNsResponse?.ResponseCode === 0 || resp.data?.SetNsResponse?.ResponseCode === "0";
  } catch (err: any) {
    log("err", `Dynadot set_ns failed: ${err.message}`);
    return false;
  }
}

// ─────────────────────────────────────────────────────────────────────
// Dynadot Domain Purchase
// ─────────────────────────────────────────────────────────────────────

async function purchaseDomain(domain: string): Promise<boolean> {
  if (!DYNADOT_KEY) { log("err", "No Dynadot API key"); return false; }
  try {
    const params = new URLSearchParams({ key: DYNADOT_KEY, command: "register", domain, duration: "1" });
    const resp = await axios.get(`${DYNADOT_API}?${params}`, { timeout: 60000 });
    const code = resp.data?.RegisterResponse?.ResponseCode;
    if (code === 0 || code === "0") {
      log("ok", `Dynadot purchase success: ${domain}`);
      return true;
    }
    // Already registered by us?
    const status = resp.data?.RegisterResponse?.Status || "";
    if (status.includes("already") || status.includes("own") || status === "not_available") {
      log("warn", `Domain already registered (likely ours): ${domain}`);
      return true;
    }
    // system_busy = transient, retry once after 5s
    if (code === 5 || status === "system_busy") {
      log("warn", "Dynadot system_busy, retrying in 5s...");
      await new Promise(r => setTimeout(r, 5000));
      const resp2 = await axios.get(`${DYNADOT_API}?${params}`, { timeout: 60000 });
      const code2 = resp2.data?.RegisterResponse?.ResponseCode;
      if (code2 === 0 || code2 === "0") { log("ok", `Dynadot purchase success (retry): ${domain}`); return true; }
    }
    log("err", `Dynadot purchase failed: ${JSON.stringify(resp.data?.RegisterResponse)}`);
    return false;
  } catch (err: any) {
    log("err", `Dynadot purchase error: ${err.message}`);
    return false;
  }
}

// ─────────────────────────────────────────────────────────────────────
// Main pipeline
// ─────────────────────────────────────────────────────────────────────

async function processDomain(domainId: string, actionId: string) {
  // Fetch domain
  const { data: domain, error: domErr } = await supabase
    .from("domains")
    .select("*")
    .eq("id", domainId)
    .single();

  if (domErr || !domain) {
    log("err", `Domain not found: ${domErr?.message}`);
    process.exit(1);
  }

  const customerId = domain.customer_id;
  let history = domain.action_history || "";

  log("info", `Part 1: Processing domain ${domain.domain} (source: ${domain.source})`);

  // ────────────────────────────────────────────────────────────────
  // Step 0: Purchase domain via Dynadot (buy orders only)
  // ────────────────────────────────────────────────────────────────
  if (domain.source === "buy" && domain.interim_status === "Both - New Order") {
    log("info", "Step 0: Purchasing domain via Dynadot...");
    const purchased = await purchaseDomain(domain.domain);
    if (purchased) {
      history = addHistory(history, `Domain purchased via Dynadot: ${domain.domain}`);
      await addActionLog(actionId, domainId, customerId, "domain_purchased", "info", `Purchased: ${domain.domain}`);
      log("ok", `Domain purchased: ${domain.domain}`);
    } else {
      history = addHistory(history, `FAILED: Could not purchase domain ${domain.domain}`);
      await updateDomain(domainId, { action_history: history, interim_status: "Both - Failed" });
      log("err", `Failed to purchase domain: ${domain.domain}`);
      console.log("PART1_RESULT:FAILED:purchase");
      process.exit(1);
    }
  } else if (domain.source === "buy") {
    log("info", "Step 0: Domain already purchased, skipping");
  } else {
    log("info", "Step 0: BYOD domain, no purchase needed");
  }

  // ────────────────────────────────────────────────────────────────
  // Step 1: Cloudflare Zone
  // ────────────────────────────────────────────────────────────────
  let zoneId = domain.cloudflare_zone_id;

  if (!zoneId) {
    log("info", "Step 1: Creating Cloudflare zone...");
    zoneId = await getOrCreateCloudflareZone(domain.domain);

    if (zoneId) {
      await updateDomain(domainId, { cloudflare_zone_id: zoneId, interim_status: "Both - DNS Zone Created" });
      history = addHistory(history, `Cloudflare zone created: ${zoneId}`);
      await addActionLog(actionId, domainId, customerId, "cf_zone_created", "info", `Zone created: ${zoneId}`);
      log("ok", `Cloudflare zone: ${zoneId}`);
    } else {
      history = addHistory(history, "FAILED: Could not create Cloudflare zone");
      await updateDomain(domainId, { action_history: history, interim_status: "Both - Failed" });
      log("err", "Failed to create Cloudflare zone");
      // Output error for PowerShell to read
      console.log("PART1_RESULT:FAILED:cf_zone");
      process.exit(1);
    }
  } else {
    log("info", `Step 1: Cloudflare zone already exists: ${zoneId}`);
  }

  // ────────────────────────────────────────────────────────────────
  // Step 2: NS Migration to Cloudflare (purchased domains only)
  // ────────────────────────────────────────────────────────────────
  if (domain.source === "buy" && !domain.nameservers_moved) {
    log("info", "Step 2: Moving nameservers to Cloudflare...");

    const nameservers = await getCloudflareNameservers(zoneId);
    if (nameservers && nameservers.length >= 2) {
      const success = await setDynadotNameservers(domain.domain, nameservers[0], nameservers[1]);
      if (success) {
        await updateDomain(domainId, { nameservers_moved: true, interim_status: "Both - NS Migrated" });
        history = addHistory(history, `NS migrated to Cloudflare: ${nameservers[0]}, ${nameservers[1]}`);
        await addActionLog(actionId, domainId, customerId, "ns_migrated", "info", `NS: ${nameservers[0]}, ${nameservers[1]}`);
        log("ok", `NS moved: ${nameservers[0]}, ${nameservers[1]}`);
      } else {
        history = addHistory(history, "FAILED: NS migration failed");
        await updateDomain(domainId, { action_history: history, interim_status: "Both - Failed" });
        log("err", "NS migration failed");
        console.log("PART1_RESULT:FAILED:ns_migration");
        process.exit(1);
      }
    } else {
      log("warn", "Cloudflare zone has no nameservers assigned yet");
    }
  } else if (domain.source === "buy") {
    log("info", "Step 2: NS already moved, skipping");
  } else {
    log("info", "Step 2: BYOD domain — user manages NS, skipping");
  }

  // ────────────────────────────────────────────────────────────────
  // Step 3: Wait for Cloudflare zone to become active
  // ────────────────────────────────────────────────────────────────
  if (domain.source === "buy") {
    log("info", "Step 3: Waiting for Cloudflare zone to become active...");
    const maxWait = 600; // 10 minutes max
    const interval = 20;
    let active = false;
    for (let elapsed = 0; elapsed < maxWait; elapsed += interval) {
      active = await isCloudflareZoneActive(zoneId);
      if (active) {
        log("ok", `Step 3: Cloudflare zone is active (took ${elapsed}s)`);
        history = addHistory(history, "Cloudflare zone is active");
        break;
      }
      if (elapsed === 0) log("info", "  Polling every 20s for NS propagation...");
      await new Promise(r => setTimeout(r, interval * 1000));
    }
    if (!active) {
      log("warn", "Step 3: CF zone still pending after 10 min. Part 2 will need to wait.");
      history = addHistory(history, "WARNING: CF zone still pending after 10 min wait");
    }
  } else {
    const active = await isCloudflareZoneActive(zoneId);
    if (active) {
      log("ok", "Step 3: Cloudflare zone is active");
    } else {
      log("warn", "Step 3: Cloudflare zone not active — BYOD user needs to update NS");
    }
  }

  // Save history
  await updateDomain(domainId, { action_history: history });

  // Output success for PowerShell to read
  log("ok", `Part 1 complete for ${domain.domain}`);
  console.log(`PART1_RESULT:SUCCESS:${zoneId}`);
}

function addHistory(history: string, entry: string): string {
  const ts = new Date().toISOString().replace("T", " ").slice(0, 19);
  const line = `[${ts}] ${entry}`;
  return history ? `${history}\n${line}` : line;
}

// ─────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
if (args.length < 2) {
  console.error("Usage: npx tsx Part1-UniversalDomainSetup.ts <domain_id> <action_id>");
  process.exit(1);
}

processDomain(args[0], args[1]).catch((err) => {
  console.error(`Part 1 fatal error: ${err.message}`);
  console.log("PART1_RESULT:FAILED:fatal");
  process.exit(1);
});
