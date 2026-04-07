/**
 * Seeds a fresh BYOD order with a new domain and immediately marks it
 * so only the PowerShell pipeline processes it (not the TS worker).
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

const DOMAIN = "testrmflow2026.xyz";
const CUSTOMER_ID = "aa46a422-aab6-49d6-ad18-0a0b842106bb";

async function seed() {
  console.log(`Seeding NEW order for ${DOMAIN}...\n`);

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

  // Create domain (BYOD, no CF zone yet - Part 1 will create it)
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
      nameservers_moved: true,
    })
    .select()
    .single();

  if (domErr) { console.error("Domain error:", domErr.message); process.exit(1); }
  console.log("Domain:", domain.id);

  // 3 inboxes — ALL "James Wilson" to test duplicate display name handling
  const usernames = ["j.wilson", "jwilson", "james.w"];
  for (const username of usernames) {
    const email = `${username}@${DOMAIN}`;
    const { data, error } = await sb
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

    if (error) console.error(`Inbox ${email}:`, error.message);
    else console.log(`Inbox: ${data.email} (${data.id})`);
  }

  // Create action with status "in_progress" so the TS worker doesn't grab it
  // (TS worker looks for "pending" actions)
  const { data: action, error: actErr } = await sb
    .from("actions")
    .insert({
      type: "provision_inbox",
      status: "in_progress",
      domain_id: domain.id,
      customer_id: CUSTOMER_ID,
      order_batch_id: batch.id,
      started_at: new Date().toISOString(),
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

  console.log(`\n=== SEEDED ===`);
  console.log(`Domain:   ${DOMAIN} (${domain.id})`);
  console.log(`Action:   ${action.id} (status: in_progress)`);
  console.log(`Inboxes:  3x "James Wilson"`);
  console.log(`\nDomain ID: ${domain.id}`);
  console.log(`Action ID: ${action.id}`);
}

seed();
