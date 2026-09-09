// records-data — the Firm Brokers payroll history, precomputed.
//
// WHY: the Records Room used to scan three weeks of chain logs in every
// visitor's browser. The official RPC limits per IP, phones share their
// carrier's address, and its 429 is unreadable by browsers — so phones got
// "could not read the chain". This scanner reads the chain ONCE, gently, and
// publishes plain JSON on GitHub Pages; the page reads JSON and asks the chain
// only for the last hour.
//
// WHAT it writes (all under data/):
//   head.json          { head, at, rounds, tokens, wallets }   the block the data is complete to
//   rounds.json        [[round, pot, totalWeight, block], …]   every RoundSettled, ascending
//   t/<id>.json        { t: transfers [[from, to, block, logIndex]], s: syncs [[weight, liveFrom, block, logIndex]],
//                        d: deliveries [[asset, ethIn, out, block, tx, logIndex]] }   per broker, ascending
//   w/<address>.json   { ids: [tokenId, …] }   every broker a wallet ever RECEIVED (lower-case address)
// Numbers that can exceed 2^53 are strings. Files are rewritten only when they change.
//
// HOW: incremental from head.json (or the engine's deploy block), in pages that
// halve on any error and grow back after a success, one request in flight,
// paced; the last SAFE blocks are left for the next run so a reorg never lands
// in the data.   node scan.mjs   (env RPC=… to override the node)
import fs from "node:fs";
import path from "node:path";

const RPC = process.env.RPC || "https://rpc.mainnet.chain.robinhood.com";
const ENGINE = "0x5a362ffdab7ffa585d50f1a5c032288ef0029740";
const NFT = "0x2d4dff47ba18c89847faca0c968e073d8b70abb4";
const DEPLOY_BLOCK = 48_370_000;
const SAFE = 64;
const PAGE = 750_000, MIN_PAGE = 25_000;
const GAP_MS = 800;
const TOPIC = {
  SETTLED: "0x866f813a2289b14a1e94be9b6a7db4b5ad759df3fb1466245f650642f3cc7a56",
  SYNCED: "0x9aa1a56064c83c34d45ce0f34a60a04b6c6fd4bf28b61a19c16235ebedb30b19",
  DELIVERED: "0x8110a247e3bf84088ca20c991ad431b68293ca3bdfe626df91b9744bf4d7b9ce",
  TRANSFER: "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
};
const DATA = path.resolve("data");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const hex = (n) => "0x" + n.toString(16);
const w = (data, i) => data.slice(2 + i * 64, 2 + (i + 1) * 64);
const big = (data, i) => BigInt("0x" + w(data, i));
const num = (h) => Number(BigInt(h));
const addr = (topic) => "0x" + topic.slice(26).toLowerCase();

let lastAt = 0, calls = 0, retries = 0;
async function rpc(method, params) {
  const wait = lastAt + GAP_MS - Date.now();
  if (wait > 0) await sleep(wait);
  const WAITS = [0, 2000, 4000, 8000, 16000, 32000];
  let last;
  for (let a = 0; a < WAITS.length; a++) {
    if (a) { retries++; await sleep(WAITS[a]); }
    try {
      const r = await fetch(RPC, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) });
      lastAt = Date.now(); calls++;
      if (r.status === 429) { last = new Error("429"); continue; }
      if (!r.ok) throw new Error("http " + r.status);
      const j = await r.json();
      if (j.error) { const e = new Error(j.error.message || "rpc error"); e.rpc = true; throw e; }
      return j.result;
    } catch (e) {
      last = e;
      if (e.rpc) throw e; // a node complaint (range too big / timed out) goes to the caller to halve on
    }
  }
  throw last;
}
const tooBig = (e) => /more than|too many|timed out|too large|exceed|limit/i.test(String(e && e.message || e));

/// every log of `filter` in [from, to], halving on a complaint, growing back on success
async function scan(filter, from, to, onPage) {
  const out = [];
  let page = PAGE;
  for (let a = from; a <= to;) {
    page = Math.min(page, to - a + 1);
    const b = a + page - 1;
    try {
      const logs = await rpc("eth_getLogs", [{ ...filter, fromBlock: hex(a), toBlock: hex(b) }]);
      for (const l of logs) out.push(l);
      if (onPage) onPage(a, b, logs.length);
      a = b + 1;
      page = Math.min(PAGE, page * 2);
    } catch (e) {
      if (!tooBig(e) && !/429/.test(String(e.message))) throw e;
      if (page <= MIN_PAGE) throw e;
      page = Math.max(MIN_PAGE, Math.floor(page / 2));
    }
  }
  return out;
}

const readJson = (p, dflt) => { try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch (e) { return dflt; } };
let written = 0;
function writeIfChanged(p, obj) {
  const s = JSON.stringify(obj);
  try { if (fs.readFileSync(p, "utf8") === s) return; } catch (e) { /* new */ }
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, s); written++;
}
const dedupeSort = (rows, blockIdx, liIdx) => {
  const seen = new Set(); const out = [];
  for (const r of rows) { const k = r[blockIdx] + ":" + r[liIdx]; if (seen.has(k)) continue; seen.add(k); out.push(r); }
  return out.sort((x, y) => x[blockIdx] - y[blockIdx] || x[liIdx] - y[liIdx]);
};

async function main() {
  const t0 = Date.now();
  const state = readJson(path.join(DATA, "head.json"), null);
  const chainHead = num(await rpc("eth_blockNumber", []));
  const from = state ? state.head + 1 : DEPLOY_BLOCK;
  const to = chainHead - SAFE;
  if (to < from) { console.log(`nothing new: data head ${state.head}, chain ${chainHead}`); return; }
  console.log(`scanning ${from} → ${to} (${to - from + 1} blocks; data ${state ? "since " + state.head : "from the deploy block"})`);
  const onPage = (tag) => (a, b, n) => console.log(`  ${tag} ${a}-${b} logs=${n}`);

  // 1. the settled hours
  const rounds = readJson(path.join(DATA, "rounds.json"), []);
  const settled = await scan({ address: ENGINE, topics: [TOPIC.SETTLED] }, from, to, onPage("settled"));
  for (const l of settled) rounds.push([num(l.topics[1]), big(l.data, 0).toString(), big(l.data, 1).toString(), num(l.blockNumber)]);
  { const seen = new Set(); const uniq = rounds.filter((r) => !seen.has(r[0]) && seen.add(r[0])).sort((x, y) => x[3] - y[3]); writeIfChanged(path.join(DATA, "rounds.json"), uniq); rounds.length = 0; rounds.push(...uniq); }

  // 2. every broker's engine events, in one filter
  const touched = new Map(); // id → { t, s, d } (new rows)
  const bucket = (id) => { let b = touched.get(id); if (!b) { b = { t: [], s: [], d: [] }; touched.set(id, b); } return b; };
  const engine = await scan({ address: ENGINE, topics: [[TOPIC.SYNCED, TOPIC.DELIVERED]] }, from, to, onPage("engine"));
  for (const l of engine) {
    const id = num(l.topics[1]), block = num(l.blockNumber), li = num(l.logIndex);
    if (l.topics[0] === TOPIC.SYNCED) bucket(id).s.push([big(l.data, 0).toString(), num(BigInt("0x" + w(l.data, 1))), block, li]);
    else bucket(id).d.push([num(l.topics[2]), big(l.data, 0).toString(), big(l.data, 1).toString(), block, l.transactionHash, li]);
  }
  // 3. every broker's transfers, and who received what
  const wallets = new Map(); // address → Set(ids) (new)
  const transfers = await scan({ address: NFT, topics: [TOPIC.TRANSFER] }, from, to, onPage("transfer"));
  for (const l of transfers) {
    if (l.topics.length < 4) continue; // not an ERC-721 transfer
    const id = num(l.topics[3]), f = addr(l.topics[1]), t = addr(l.topics[2]);
    bucket(id).t.push([f, t, num(l.blockNumber), num(l.logIndex)]);
    if (!wallets.has(t)) wallets.set(t, new Set()); wallets.get(t).add(id);
  }
  // merge into the per-token and per-wallet files
  for (const [id, b] of touched) {
    const p = path.join(DATA, "t", id + ".json");
    const cur = readJson(p, { t: [], s: [], d: [] });
    writeIfChanged(p, { t: dedupeSort(cur.t.concat(b.t), 2, 3), s: dedupeSort(cur.s.concat(b.s), 2, 3), d: dedupeSort(cur.d.concat(b.d), 3, 5) });
  }
  for (const [a, ids] of wallets) {
    const p = path.join(DATA, "w", a + ".json");
    const cur = readJson(p, { ids: [] });
    const merged = [...new Set(cur.ids.concat([...ids]))].sort((x, y) => x - y);
    writeIfChanged(p, { ids: merged });
  }
  const tokens = fs.existsSync(path.join(DATA, "t")) ? fs.readdirSync(path.join(DATA, "t")).length : 0;
  const walletFiles = fs.existsSync(path.join(DATA, "w")) ? fs.readdirSync(path.join(DATA, "w")).length : 0;
  writeIfChanged(path.join(DATA, "head.json"), { head: to, at: new Date().toISOString(), rounds: rounds.length, tokens, wallets: walletFiles });
  console.log(`done: head ${to} · rounds ${rounds.length} · settled +${settled.length} · engine +${engine.length} · transfers +${transfers.length} · tokens ${tokens} · wallets ${walletFiles} · files written ${written} · rpc calls ${calls} (retries ${retries}) · ${((Date.now() - t0) / 1000).toFixed(0)}s`);
}
main().catch((e) => { console.error("scan failed:", e && e.stack || e); process.exit(1); });
