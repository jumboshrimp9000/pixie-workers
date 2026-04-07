/**
 * Seeds a BYOD test order reusing the existing domain testinboxflow2026.xyz
 * Creates 3 new inboxes all named "James Wilson" to test duplicate name handling.
 */
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, ".env") });

const sb = createClient(
  process.env.SUPABASE_URL || "",
  process.env.SUPABASE_SERVICE_ROLE_KEY || "",
  { auth: { autoRefreshToken: false, persistSession: false } }
);

const DOMAIN = "testinboxflow2026.xyz";
const CUSTOMER_ID = "aa46a422-aab6-49d6-ad18-0a0b842106bb";
const CF_ZONE_ID = "c7d9a831cc59c0180e07876cd86d675a";

async function seed() {
  console.log(`Seeding BYOD order for ${DOMAIN}...\n`);

  // Create order batch
  const { data: batch, error: batchErr } = await sb
    .from("order_batches")
    .insert({
      customer_id: CUSTOMER_ID,
      status: "processing",
      total_inboxes: 3,
      provider: "microsoft",
    })
    .select()
    .single();

  if (batchErr) { console.error("Batch error:", batchErr.message); process.exit(1); }
  console.log("Order batch:", batch.id);

  // Create domain (BYOD - source 'own', CF zone already exists)
  const { data: domain, error: domErr } = await sb
    .from("domains")
    .insert({
      order_batch_id: batch.id,
      customer_id: CUSTOMER_ID,
      domain: DOMAIN,
      provider: "microsoft",
      source: "own",
      status: "pending",
      interim_status: "Both - New Order",
      nameservers_moved: true, // BYOD - user manages NS
      cloudflare_zone_id: CF_ZONE_ID, // Already exists
    })
    .select()
    .single();

  if (domErr) { console.error("Domain error:", domErr.message); process.exit(1); }
  console.log("Domain:", domain.id);

  // Create 3 inboxes - ALL named "James Wilson" to test duplicate handling
  const usernames = ["j.wilson", "jwilson", "james.w"];
  for (const username of usernames) {
    const email = `${username}@${DOMAIN}`;
    const { data: inbox, error: inboxErr } = await sb
      .from("inboxes")
      .insert({
        domain_id: domain.id,
        customer_id: CUSTOMER_ID,
        email,
        username,
        first_name: "James",
        last_name: "Wilson",
        status: "pending",
      })
      .select()
      .single();

    if (inboxErr) { console.error(`Inbox ${email} error:`, inboxErr.message); }
    else { console.log(`Inbox: ${inbox.email} (${inbox.id})`); }
  }

  // Create provision_inbox action
  const { data: action, error: actErr } = await sb
    .from("actions")
    .insert({
      type: "provision_inbox",
      status: "pending",
      domain_id: domain.id,
      customer_id: CUSTOMER_ID,
      order_batch_id: batch.id,
      payload: {
        domain: DOMAIN,
        provider: "microsoft",
        source: "own",
        inbox_count: 3,
      },
    })
    .select()
    .single();

  if (actErr) { console.error("Action error:", actErr.message); process.exit(1); }
  console.log("Action:", action.id);

  console.log("\n=== SEEDED ===");
  console.log(`Domain ID:  ${domain.id}`);
  console.log(`Action ID:  ${action.id}`);
  console.log(`Inboxes:    3x "James Wilson" (j.wilson, jwilson, james.w)`);
  console.log(`\nRun: pwsh ./run.ps1 -Once`);
}

seed();
