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
  // Check pending actions
  const { data: actions } = await sb
    .from("actions")
    .select("*")
    .in("status", ["pending", "in_progress"])
    .eq("type", "provision_inbox")
    .limit(5);
  console.log("Pending provision_inbox actions:", actions?.length || 0);
  if (actions?.length) console.log(JSON.stringify(actions, null, 2));

  // Check domains
  const { data: domains } = await sb
    .from("domains")
    .select("id, domain, provider, interim_status, source, cloudflare_zone_id")
    .limit(10);
  console.log("\nDomains in table:", domains?.length || 0);
  if (domains?.length) console.log(JSON.stringify(domains, null, 2));

  // Check admin_credentials
  const { data: admins } = await sb
    .from("admin_credentials")
    .select("id, email, provider, active")
    .limit(5);
  console.log("\nAdmin credentials:", admins?.length || 0);
  if (admins?.length) console.log(JSON.stringify(admins, null, 2));

  // Check inboxes
  const { data: inboxes } = await sb
    .from("inboxes")
    .select("id, domain_id, email, display_name, status")
    .eq("status", "pending")
    .limit(10);
  console.log("\nPending inboxes:", inboxes?.length || 0);
  if (inboxes?.length) console.log(JSON.stringify(inboxes, null, 2));
}

check();
