/**
 * Resets testinboxflow2026.xyz for a fresh room mailbox test.
 * This domain is registered, NS points to CF, zone is active.
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
const CF_ZONE = "c7d9a831cc59c0180e07876cd86d675a";

async function seed() {
  console.log(`Resetting ${DOMAIN} for room mailbox test...\n`);

  // 1. Reset domain — keep CF zone, mark as BYOD pending
  const { error: domErr } = await sb.from("domains").update({
    source: "own",
    status: "pending",
    interim_status: "Both - DNS Zone Created",
    action_history: null,
    updated_at: new Date().toISOString(),
  }).eq("id", DOMAIN_ID);
  if (domErr) { console.error("Domain error:", domErr.message); process.exit(1); }
  console.log("Domain reset (zone active, skipping Part 1)");

  // 2. Create 3 new pending inboxes — all "James Wilson" for duplicate test
  const usernames = ["j.wilson", "jwilson", "james.w"];
  for (const u of usernames) {
    const email = `${u}@${DOMAIN}`;
    // Delete if exists from previous attempt
    await sb.from("inboxes").delete().eq("email", email);
    const { data, error } = await sb.from("inboxes").insert({
      domain_id: DOMAIN_ID,
      customer_id: CUSTOMER_ID,
      email,
      username: u,
      first_name: "James",
      last_name: "Wilson",
      status: "pending",
    }).select().single();
    if (error) console.error(`${email}:`, error.message);
    else console.log(`Inbox: ${data.email} (${data.id})`);
  }

  // 3. Create action (in_progress so TS worker doesn't grab it)
  // First clean up any old pending/in_progress actions for this domain
  await sb.from("actions").update({ status: "completed" })
    .eq("domain_id", DOMAIN_ID).in("status", ["pending", "in_progress"]);

  const { data: action, error: actErr } = await sb.from("actions").insert({
    type: "provision_inbox",
    status: "in_progress",
    domain_id: DOMAIN_ID,
    customer_id: CUSTOMER_ID,
    started_at: new Date().toISOString(),
    payload: { domain: DOMAIN, provider: "microsoft", source: "own", inbox_count: 3 },
  }).select().single();

  if (actErr) { console.error("Action error:", actErr.message); process.exit(1); }

  console.log(`\n=== READY ===`);
  console.log(`Domain: ${DOMAIN} (active CF zone: ${CF_ZONE})`);
  console.log(`Action: ${action.id}`);
  console.log(`Inboxes: 3x "James Wilson" — tests duplicate display name handling`);
  console.log(`\nRun: pwsh ./run.ps1 -Once`);
}

seed();
