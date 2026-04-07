/**
 * Resets the existing testinboxflow2026.xyz domain to simulate a fresh BYOD order.
 * - Resets domain status to pending
 * - Adds 3 new "James Wilson" inboxes (pending)
 * - Creates a new provision_inbox action
 * - Keeps existing CF zone ID (simulates BYOD where zone already exists)
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

const DOMAIN_ID = "b208ec9f-ea70-45b7-bf16-f91920b2dda2";
const CUSTOMER_ID = "aa46a422-aab6-49d6-ad18-0a0b842106bb";
const DOMAIN = "testinboxflow2026.xyz";

async function resetAndSeed() {
  console.log("Resetting domain for BYOD test...\n");

  // 1. Reset domain to pending BYOD state (keep CF zone)
  const { error: domErr } = await sb
    .from("domains")
    .update({
      source: "own",
      status: "pending",
      interim_status: "Both - New Order",
      action_history: null,
    })
    .eq("id", DOMAIN_ID);

  if (domErr) { console.error("Domain reset error:", domErr.message); process.exit(1); }
  console.log("Domain reset to BYOD pending state");

  // 2. Mark existing inboxes as 'previous' so they don't interfere
  // (the existing james.wilson, sarah.chen, mike.torres are regular users, not room mailboxes)
  const { error: inboxUpdateErr } = await sb
    .from("inboxes")
    .update({ status: "active_legacy" })
    .eq("domain_id", DOMAIN_ID)
    .eq("status", "active");

  if (inboxUpdateErr) console.warn("Inbox update warning:", inboxUpdateErr.message);
  else console.log("Existing inboxes marked as active_legacy");

  // 3. Create 3 new inboxes - ALL "James Wilson" to test duplicate display name handling
  const newInboxes = [
    { username: "j.wilson", first_name: "James", last_name: "Wilson" },
    { username: "jwilson", first_name: "James", last_name: "Wilson" },
    { username: "james.w", first_name: "James", last_name: "Wilson" },
  ];

  const inboxIds: string[] = [];
  for (const inbox of newInboxes) {
    const email = `${inbox.username}@${DOMAIN}`;
    const { data, error } = await sb
      .from("inboxes")
      .insert({
        domain_id: DOMAIN_ID,
        customer_id: CUSTOMER_ID,
        email,
        username: inbox.username,
        first_name: inbox.first_name,
        last_name: inbox.last_name,
        status: "pending",
      })
      .select()
      .single();

    if (error) { console.error(`Inbox ${email} error:`, error.message); }
    else { console.log(`Created inbox: ${data.email} (${data.id})`); inboxIds.push(data.id); }
  }

  // 4. Mark old action as archived
  const { error: actArchErr } = await sb
    .from("actions")
    .update({ status: "archived" })
    .eq("domain_id", DOMAIN_ID)
    .eq("status", "completed");

  if (actArchErr) console.warn("Action archive warning:", actArchErr.message);
  else console.log("Old completed action archived");

  // 5. Create new provision_inbox action
  const { data: action, error: actErr } = await sb
    .from("actions")
    .insert({
      type: "provision_inbox",
      status: "pending",
      domain_id: DOMAIN_ID,
      customer_id: CUSTOMER_ID,
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
  console.log(`Created action: ${action.id}`);

  // Also clean up the orphan batch from the failed seed attempt
  await sb.from("order_batches").delete().eq("id", "8c2aba2a-42d1-41fe-8f19-c23c1b89b8bf");

  console.log("\n=== READY FOR E2E TEST ===");
  console.log(`Domain:    ${DOMAIN} (${DOMAIN_ID})`);
  console.log(`Action:    ${action.id}`);
  console.log(`Inboxes:   3x "James Wilson" (j.wilson, jwilson, james.w) — all PENDING`);
  console.log(`Source:    own (BYOD)`);
  console.log(`CF Zone:   c7d9a831cc59c0180e07876cd86d675a (already exists)`);
  console.log(`\nRun: pwsh ./run.ps1 -Once`);
}

resetAndSeed();
