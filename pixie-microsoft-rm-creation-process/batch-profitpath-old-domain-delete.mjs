import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const scriptPath = path.join(__dirname, "one-off-profitpath-old-domain-delete.ps1");

function argValue(name, fallback = "") {
  const index = process.argv.indexOf(name);
  if (index === -1 || index + 1 >= process.argv.length) return fallback;
  return process.argv[index + 1];
}

function hasFlag(name) {
  return process.argv.includes(name);
}

function normalizeDomain(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0]
    .replace(/^\.+|\.+$/g, "");
}

const manifestPath = argValue("--manifest");
const live = hasFlag("--live");
const concurrency = Math.max(1, Number(argValue("--concurrency", "1")) || 1);
const expectedNewUserCount = Math.max(1, Number(argValue("--expected-new-user-count", "99")) || 99);
const confirm = argValue("--confirm");
const limit = Math.max(0, Number(argValue("--limit", "0")) || 0);
const stopOnFailure = hasFlag("--stop-on-failure");

if (!manifestPath) {
  throw new Error("Usage: node batch-profitpath-old-domain-delete.mjs --manifest path.json [--live --confirm \"DELETE PROFITPATH OLD DOMAINS\"] [--concurrency 3]");
}
if (live && confirm !== "DELETE PROFITPATH OLD DOMAINS") {
  throw new Error('Live batch deletion requires --confirm "DELETE PROFITPATH OLD DOMAINS"');
}

const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
const pairs = (Array.isArray(manifest) ? manifest : manifest.pairs || [])
  .map((row) => ({
    row: Number(row.row || 0),
    oldDomain: normalizeDomain(row.oldDomain || row.old || row.old_domain),
    newDomain: normalizeDomain(row.newDomain || row.new || row.new_domain),
  }))
  .filter((row) => row.row && row.oldDomain && row.newDomain && row.oldDomain !== row.newDomain);

if (pairs.length === 0) {
  throw new Error(`No valid old/new pairs found in ${manifestPath}`);
}

const selectedPairs = limit > 0 ? pairs.slice(0, limit) : pairs;
const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
const batchDir = path.join(__dirname, "logs", `profitpath-old-delete-batch-${live ? "live" : "dryrun"}-${stamp}`);
fs.mkdirSync(batchDir, { recursive: true });

let nextIndex = 0;
let completed = 0;
let failed = 0;
let stopRequested = false;
const results = [];

function runPair(pair) {
  return new Promise((resolve) => {
    const logPath = path.join(batchDir, `${String(pair.row).padStart(3, "0")}-${pair.oldDomain}.log`);
    const log = fs.createWriteStream(logPath, { flags: "a" });
    const args = [
      "-NoLogo",
      "-NoProfile",
      "-File",
      scriptPath,
      "-OldDomain",
      pair.oldDomain,
      "-NewDomain",
      pair.newDomain,
      "-ExpectedNewUserCount",
      String(expectedNewUserCount),
    ];
    if (live) {
      args.push("-Live", "-ConfirmText", `DELETE OLD DOMAIN ${pair.oldDomain} KEEP NEW ${pair.newDomain}`);
    }

    const startedAt = new Date().toISOString();
    console.log(`[start] row ${pair.row}: ${pair.oldDomain} -> ${pair.newDomain}`);
    const child = spawn("pwsh", args, { cwd: __dirname, stdio: ["ignore", "pipe", "pipe"] });
    child.stdout.on("data", (data) => log.write(data));
    child.stderr.on("data", (data) => log.write(data));
    child.on("close", (code) => {
      log.end();
      const finishedAt = new Date().toISOString();
      const result = {
        ...pair,
        status: code === 0 ? "completed" : "failed",
        exitCode: code,
        startedAt,
        finishedAt,
        logPath,
      };
      results.push(result);
      completed += 1;
      if (code !== 0) {
        failed += 1;
        if (stopOnFailure) stopRequested = true;
      }
      console.log(`[${result.status}] ${completed}/${selectedPairs.length} row ${pair.row}: ${pair.oldDomain} -> ${pair.newDomain}`);
      resolve(result);
    });
  });
}

async function worker() {
  while (nextIndex < selectedPairs.length) {
    if (stopRequested) return;
    const pair = selectedPairs[nextIndex];
    nextIndex += 1;
    await runPair(pair);
  }
}

await Promise.all(Array.from({ length: Math.min(concurrency, selectedPairs.length) }, () => worker()));

const summary = {
  mode: live ? "live" : "dry_run",
  manifestPath,
  batchDir,
  requested: selectedPairs.length,
  completed: results.filter((row) => row.status === "completed").length,
  failed,
  results,
};
const summaryPath = path.join(batchDir, "summary.json");
fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
console.log(JSON.stringify({
  mode: summary.mode,
  requested: summary.requested,
  completed: summary.completed,
  failed: summary.failed,
  batchDir,
  summaryPath,
}, null, 2));

if (failed > 0) {
  process.exitCode = 1;
}
