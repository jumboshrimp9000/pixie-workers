/**
 * Seeds a REAL end-to-end test: purchase domain → CF zone → NS → M365 → room mailboxes.
 * Source = "buy" so Part 1 purchases via Dynadot + creates CF zone + migrates NS.
 */
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import axios from "axios";

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, ".env") });

const sb = createClient(
  process.env.SUPABASE_URL || "",
  process.env.SUPABASE_SERVICE_ROLE_KEY || "",
  { auth: { autoRefreshToken: false, persistSession: false } }
);

const DYNADOT_KEY = process.env.DYNADOT_API_KEY || "";
const CUSTOMER_ID = "aa46a422-aab6-49d6-ad18-0a0b842106bb";
const DOMAIN = "testrmboxes2026.xyz";

async function checkAvailability() {
  const params = new URLSearchParams({ key: DYNADOT_KEY, command: "search", domain0: DOMAIN });
  const resp = await axios.get(`https://api.dynadot.com/api3.json?${params}`, { timeout: 15000 });
  const result = resp.data?.SearchResponse?.SearchResults?.[0];
  console.log(`Domain: ${DOMAIN}`);
  console.log(`Available: ${result?.Available === "yes"}`);
  if (result?.Available !== "yes") {
    console.error("Domain not available! Pick a different name.");
    process.exit(1);
  }
  console.log(`Price: $${(result?.Price || 0) / 100}`);
}

async function seed() {
  console.log(`Setting up FULL E2E test: BUY ${DOMAIN}\n`);

  await checkAvailability();

  // Create order batch
  const { data: batch, error: batchErr } = await sb.from("order_batches").insert({
    customer_id: CUSTOMER_ID,
    status: "processing",
    total_inboxes: 3,
    provider: "microsoft",
  }).select().single();
  if (batchErr) { console.error("Batch:", batchErr.message); process.exit(1); }

  // Create domain — source "buy", NO cloudflare_zone_id, NS not moved
  const { data: domain, error: domErr } = await sb.from("domains").insert({
    order_batch_id: batch.id,
    customer_id: CUSTOMER_ID,
    domain: DOMAIN,
    provider: "microsoft",
    source: "buy",
    status: "pending",
    interim_status: "Both - New Order",
    nameservers_moved: false,
  }).select().single();
  if (domErr) { console.error("Domain:", domErr.message); process.exit(1); }
  console.log(`\nDomain record: ${domain.id}`);

  // 3 inboxes — all "James Wilson" for duplicate name test
  const users = [
    { username: "j.wilson", first: "James", last: "Wilson" },
    { username: "jwilson", first: "James", last: "Wilson" },
    { username: "james.w", first: "James", last: "Wilson" },
  ];
  for (const u of users) {
    const { data, error } = await sb.from("inboxes").insert({
      domain_id: domain.id,
      customer_id: CUSTOMER_ID,
      email: `${u.username}@${DOMAIN}`,
      username: u.username,
      first_name: u.first,
      last_name: u.last,
      status: "pending",
    }).select().single();
    if (error) console.error(`Inbox ${u.username}:`, error.message);
    else console.log(`Inbox: ${data.email}`);
  }

  // Action — in_progress so TS worker doesn't grab it
  const { data: action, error: actErr } = await sb.from("actions").insert({
    type: "provision_inbox",
    status: "in_progress",
    domain_id: domain.id,
    customer_id: CUSTOMER_ID,
    order_batch_id: batch.id,
    started_at: new Date().toISOString(),
    payload: { domain: DOMAIN, provider: "microsoft", source: "buy", inbox_count: 3 },
  }).select().single();
  if (actErr) { console.error("Action:", actErr.message); process.exit(1); }

  console.log(`\n=== SEEDED (FULL E2E) ===`);
  console.log(`Domain:  ${DOMAIN} (source: buy, NO CF zone yet)`);
  console.log(`Action:  ${action.id}`);
  console.log(`Flow:    Dynadot purchase → CF zone → NS migration → M365 → Room mailboxes`);
  console.log(`\nRun: pwsh ./run.ps1 -Once`);
}

seed();
