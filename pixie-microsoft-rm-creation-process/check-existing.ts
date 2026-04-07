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
  const { data: domain } = await sb.from("domains").select("*").limit(1).single();
  console.log("DOMAIN:", JSON.stringify(domain, null, 2));

  const { data: inboxes } = await sb.from("inboxes").select("*");
  console.log("\nINBOXES:", JSON.stringify(inboxes, null, 2));

  const { data: action } = await sb.from("actions").select("*").limit(1).single();
  console.log("\nACTION:", JSON.stringify(action, null, 2));

  const { data: customer } = await sb.from("customers").select("*").limit(1).single();
  console.log("\nCUSTOMER:", JSON.stringify(customer, null, 2));

  const { data: batch } = await sb.from("order_batches").select("*").limit(1).single();
  console.log("\nORDER BATCH:", JSON.stringify(batch, null, 2));
}

check();
