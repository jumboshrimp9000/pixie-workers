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

async function run() {
  // Use rpc to run raw SQL via Supabase
  const { error } = await sb.rpc("exec_sql", {
    sql: `
      ALTER TABLE domains ADD COLUMN IF NOT EXISTS interim_status TEXT;
      ALTER TABLE domains ADD COLUMN IF NOT EXISTS action_history TEXT;
      CREATE INDEX IF NOT EXISTS idx_domains_interim_status ON domains(interim_status) WHERE interim_status IS NOT NULL;
    `
  });

  if (error) {
    console.log("rpc exec_sql not available, trying direct REST approach...");
    // Try via the Supabase DB URL if available
    const dbUrl = process.env.SUPABASE_DB_URL;
    if (!dbUrl) {
      console.error("No SUPABASE_DB_URL available. Run this SQL manually in the Supabase SQL editor:");
      console.log(`
ALTER TABLE domains ADD COLUMN IF NOT EXISTS interim_status TEXT;
ALTER TABLE domains ADD COLUMN IF NOT EXISTS action_history TEXT;
CREATE INDEX IF NOT EXISTS idx_domains_interim_status ON domains(interim_status) WHERE interim_status IS NOT NULL;
      `);
      process.exit(1);
    }
  } else {
    console.log("Migration complete!");
  }

  // Verify columns exist
  const { data } = await sb.from("domains").select("interim_status, action_history").limit(1);
  if (data !== null) {
    console.log("Verified: interim_status and action_history columns exist");
  } else {
    console.log("Columns may not exist yet - check manually");
  }
}

run();
