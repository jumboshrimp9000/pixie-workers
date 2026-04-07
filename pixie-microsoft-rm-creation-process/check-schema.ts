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
  // Get one row from each table to see actual columns
  const tables = ["order_batches", "domains", "inboxes", "actions"];
  for (const table of tables) {
    const { data, error } = await sb.from(table).select("*").limit(1);
    if (error) { console.log(`${table}: ERROR - ${error.message}`); continue; }
    if (data && data.length > 0) {
      console.log(`\n${table} columns:`, Object.keys(data[0]).join(", "));
    }
  }
}
check();
