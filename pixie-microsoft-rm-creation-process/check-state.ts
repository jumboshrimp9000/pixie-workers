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

async function check() {
  // List ALL tables and their row counts
  const tables = [
    "customers", "workspaces", "domains", "inboxes", "order_batches",
    "actions", "action_logs", "admin_credentials", "sending_tools",
    "sending_tool_credentials", "domain_credentials", "domain_admin_assignments",
    "domain_registrar_credentials", "domain_registrar_assignments",
    "dns_records", "api_keys", "transactions"
  ];

  for (const table of tables) {
    const { data, error, count } = await sb.from(table).select("*", { count: "exact", head: true });
    if (error) {
      console.log(`${table}: ERROR - ${error.message}`);
    } else {
      console.log(`${table}: ${count} rows`);
    }
  }

  // Check auth users
  const { data: authUsers } = await sb.auth.admin.listUsers();
  console.log(`\nAuth users: ${authUsers?.users?.length || 0}`);
  if (authUsers?.users) {
    for (const u of authUsers.users.slice(0, 5)) {
      console.log(`  - ${u.email} (${u.id})`);
    }
  }

  // Full admin_credentials details
  const { data: admins } = await sb.from("admin_credentials").select("*");
  console.log("\nAdmin credentials (full):");
  if (admins) {
    for (const a of admins) {
      console.log(`  ${a.email} | tenant: ${a.tenant_id || 'N/A'} | password exists: ${!!a.password}`);
    }
  }
}

check();
