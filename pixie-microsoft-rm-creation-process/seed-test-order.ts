/**
 * Seeds a test BYOD order into Supabase to test the full pipeline.
 * Simulates a user-provided domain with 3 inboxes (room mailboxes).
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

async function seed() {
  // 1. Find the admin credential to get tenant info
  const { data: admin } = await sb
    .from("admin_credentials")
    .select("*")
    .eq("provider", "microsoft")
    .eq("active", true)
    .limit(1)
    .single();

  if (!admin) {
    console.error("No active Microsoft admin credential found");
    process.exit(1);
  }
  console.log("Admin:", admin.email);

  // 2. Find a customer (or list what we have)
  const { data: customers } = await sb.from("customers").select("id, email, full_name").limit(5);
  console.log("Customers:", JSON.stringify(customers, null, 2));

  if (!customers || customers.length === 0) {
    console.error("No customers found in Supabase. Need at least one customer.");
    process.exit(1);
  }

  const customer = customers[0];
  console.log(`\nUsing customer: ${customer.email} (${customer.id})`);

  // 3. Check if there are any existing domains (from previous tests)
  const { data: existingDomains } = await sb.from("domains").select("id, domain").limit(10);
  console.log("\nExisting domains:", JSON.stringify(existingDomains, null, 2));

  // 4. Check order_batches
  const { data: batches } = await sb.from("order_batches").select("*").limit(5);
  console.log("\nExisting order batches:", JSON.stringify(batches, null, 2));

  // 5. Check what domains are on the M365 tenant already
  // We'll look at domain_admin_assignments
  const { data: assignments } = await sb.from("domain_admin_assignments").select("*").limit(10);
  console.log("\nDomain-admin assignments:", JSON.stringify(assignments, null, 2));

  console.log("\n--- Ready to seed. Run with SEED=1 to actually create records ---");

  if (process.env.SEED !== "1") {
    process.exit(0);
  }

  // === SEED THE TEST ORDER ===

  // We need a domain name. Let's check what's on the tenant.
  // For now, use a test domain - the user said "use the same domain we created"
  // Let's prompt for it
  const TEST_DOMAIN = process.env.TEST_DOMAIN;
  if (!TEST_DOMAIN) {
    console.error("Set TEST_DOMAIN env var to the domain to test with");
    process.exit(1);
  }

  console.log(`\nSeeding test order for domain: ${TEST_DOMAIN}`);

  // Create order batch
  const { data: batch, error: batchErr } = await sb
    .from("order_batches")
    .insert({
      customer_id: customer.id,
      status: "processing",
      total_domains: 1,
      total_inboxes: 3,
      provider: "microsoft",
    })
    .select()
    .single();

  if (batchErr) {
    console.error("Failed to create order batch:", batchErr.message);
    process.exit(1);
  }
  console.log("Created order batch:", batch.id);

  // Create domain record (BYOD = source 'own')
  const { data: domain, error: domErr } = await sb
    .from("domains")
    .insert({
      order_batch_id: batch.id,
      customer_id: customer.id,
      domain: TEST_DOMAIN,
      provider: "microsoft",
      source: "own",
      status: "pending",
      interim_status: "Both - New Order",
      nameservers_moved: false,
    })
    .select()
    .single();

  if (domErr) {
    console.error("Failed to create domain:", domErr.message);
    process.exit(1);
  }
  console.log("Created domain:", domain.id, domain.domain);

  // Create 3 test inboxes (same display name to test duplicate handling)
  const inboxes = [
    { display_name: "James Wilson", username: "james" },
    { display_name: "James Wilson", username: "jwilson" },
    { display_name: "James Wilson", username: "j.wilson" },
  ];

  for (const inbox of inboxes) {
    const email = `${inbox.username}@${TEST_DOMAIN}`;
    const { data: inboxRow, error: inboxErr } = await sb
      .from("inboxes")
      .insert({
        domain_id: domain.id,
        customer_id: customer.id,
        email,
        username: inbox.username,
        display_name: inbox.display_name,
        status: "pending",
      })
      .select()
      .single();

    if (inboxErr) {
      console.error(`Failed to create inbox ${email}:`, inboxErr.message);
    } else {
      console.log(`Created inbox: ${inboxRow.email} (${inboxRow.id})`);
    }
  }

  // Create the provision_inbox action
  const { data: action, error: actErr } = await sb
    .from("actions")
    .insert({
      type: "provision_inbox",
      status: "pending",
      domain_id: domain.id,
      customer_id: customer.id,
      payload: {
        domain: TEST_DOMAIN,
        provider: "microsoft",
        source: "own",
        inbox_count: 3,
      },
    })
    .select()
    .single();

  if (actErr) {
    console.error("Failed to create action:", actErr.message);
    process.exit(1);
  }
  console.log("Created action:", action.id);

  console.log("\n=== TEST ORDER SEEDED ===");
  console.log(`Domain: ${TEST_DOMAIN}`);
  console.log(`Domain ID: ${domain.id}`);
  console.log(`Action ID: ${action.id}`);
  console.log(`Inboxes: 3x "James Wilson" (testing duplicate name handling)`);
  console.log(`\nRun the pipeline: pwsh ./run.ps1 -Once`);
}

seed();
