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

async function reset() {
  // Domain already purchased, skip purchase step by advancing interim_status
  // But still need CF zone + NS migration
  await sb.from("domains").update({
    interim_status: "Both - New Order",
    action_history: "Domain purchased via Dynadot (manual retry)",
    updated_at: new Date().toISOString(),
  }).eq("id", "292fd27e-7f1d-450e-a74d-25f2be83e008");

  await sb.from("actions").update({
    status: "in_progress",
    error: null,
    result: null,
    updated_at: new Date().toISOString(),
  }).eq("id", "436c1d7d-2105-4782-99f0-ef4430074876");

  console.log("Reset. Domain purchased, pipeline will detect it and skip purchase.");
}

reset();
