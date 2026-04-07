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

const ACTION_ID = "1fdb444e-c14e-4fb4-b8cc-1ec443115fda";
const DOMAIN_ID = "79c06333-1da9-49df-bf3e-56a72cb2a986";

async function reset() {
  // Reset action to in_progress
  await sb.from("actions").update({
    status: "in_progress",
    error: null,
    result: null,
    updated_at: new Date().toISOString(),
  }).eq("id", ACTION_ID);

  // Domain already has CF zone from Part 1, so update interim_status to skip Part 1
  await sb.from("domains").update({
    interim_status: "Both - DNS Zone Created",
    updated_at: new Date().toISOString(),
  }).eq("id", DOMAIN_ID);

  console.log("Action reset to in_progress, domain set to 'Both - DNS Zone Created'");
  console.log("Part 1 will be skipped (zone exists), Part 2 will run.");
}

reset();
