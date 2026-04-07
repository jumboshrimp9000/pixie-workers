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

const DOMAIN_ID = "292fd27e-7f1d-450e-a74d-25f2be83e008";
const ACTION_ID = "436c1d7d-2105-4782-99f0-ef4430074876";

async function reset() {
  // Domain: purchased, CF zone created, NS migrated
  // Reset to NS Migrated so Part 2 picks up from verification
  await sb.from("domains").update({
    interim_status: "Both - NS Migrated",
    updated_at: new Date().toISOString(),
  }).eq("id", DOMAIN_ID);

  await sb.from("actions").update({
    status: "in_progress",
    error: null,
    result: null,
    updated_at: new Date().toISOString(),
  }).eq("id", ACTION_ID);

  // Reset inboxes to pending
  await sb.from("inboxes").update({
    status: "pending",
    password: null,
  }).eq("domain_id", DOMAIN_ID).eq("status", "pending");

  console.log("Reset to 'Both - NS Migrated'. Run: pwsh ./run.ps1 -Once");
}
reset();
