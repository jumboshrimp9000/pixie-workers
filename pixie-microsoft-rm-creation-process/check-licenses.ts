import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
import axios from "axios";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, ".env") });

const sb = createClient(
  process.env.SUPABASE_URL || "",
  process.env.SUPABASE_SERVICE_ROLE_KEY || "",
  { auth: { autoRefreshToken: false, persistSession: false } }
);

async function getToken(tenantId: string, username: string, password: string) {
  const body = new URLSearchParams({
    grant_type: "password",
    client_id: "04b07795-8ddb-461a-bbee-02f9e1bf7b46",
    scope: "https://graph.microsoft.com/.default",
    username,
    password,
  });
  const resp = await axios.post(
    `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
    body.toString(),
    { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
  );
  return resp.data.access_token;
}

async function main() {
  const { data: admin } = await sb.from("admin_credentials").select("*").eq("active", true).single();
  if (!admin) { console.error("No admin"); process.exit(1); }

  const domain = admin.email.split("@")[1];
  // Get tenant ID
  const wellKnown = await axios.get(`https://login.microsoftonline.com/${domain}/v2.0/.well-known/openid-configuration`);
  const tenantId = wellKnown.data.issuer.split("/")[3];
  console.log("Tenant:", tenantId);

  const token = await getToken(tenantId, admin.email, admin.password);
  const headers = { Authorization: `Bearer ${token}` };

  // Check subscribed SKUs (licenses)
  const skus = await axios.get("https://graph.microsoft.com/v1.0/subscribedSkus", { headers });
  console.log("\n=== LICENSES ===");
  for (const sku of skus.data.value) {
    const used = sku.consumedUnits;
    const total = sku.prepaidUnits?.enabled || 0;
    console.log(`${sku.skuPartNumber} (${sku.skuId}): ${used}/${total} used`);
  }

  // List all users
  const users = await axios.get("https://graph.microsoft.com/v1.0/users?$select=id,displayName,userPrincipalName,mail,accountEnabled&$top=50", { headers });
  console.log("\n=== USERS ===");
  for (const u of users.data.value) {
    console.log(`${u.userPrincipalName} | ${u.displayName} | enabled: ${u.accountEnabled}`);
  }

  // List users on old test domain
  console.log("\n=== USERS ON testinboxflow2026.xyz ===");
  for (const u of users.data.value) {
    if (u.userPrincipalName?.includes("testinboxflow2026.xyz")) {
      console.log(`  ${u.id} | ${u.userPrincipalName} | ${u.displayName}`);
    }
  }

  // List users on new test domain
  console.log("\n=== USERS ON testrmboxes2026.xyz ===");
  for (const u of users.data.value) {
    if (u.userPrincipalName?.includes("testrmboxes2026.xyz")) {
      console.log(`  ${u.id} | ${u.userPrincipalName} | ${u.displayName}`);
    }
  }
}

main();
