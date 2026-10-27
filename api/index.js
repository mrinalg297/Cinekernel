const { Redis } = require('@upstash/redis');
const E = process.env;
const r = new Redis({ url: E.UPSTASH_REDIS_REST_URL || E.KV_REST_API_URL, token: E.UPSTASH_REDIS_REST_TOKEN || E.KV_REST_API_TOKEN });
const sleep = ms => new Promise(o => setTimeout(o, ms));
const HALLS = { h1: 'Neon Monsoon', h2: 'The Last Compiler', h3: 'Midnight Kernel' };
const HOLD = 30000, CAP = 3, VIPB = 20, AGE = 1, QUANTUM = 2000, GONE = 12000;

const fresh = () => ({ v: 0, n: 0, act: { h1: [], h2: [], h3: [] }, q: { h1: [], h2: [], h3: [] }, booked: {}, holds: {}, vip: {},
  pay: { q: [], run: null, t: 0, sl: 0, g: [], d: [] }, algo: 'prio', unsafe: false, log: [], w: null });
const lg = (s, m) => { s.log.unshift(new Date().toISOString().slice(11, 19) + '  ' + m); s.log.length = Math.min(s.log.length, 25); s.v++; };
// priority = VIP bonus + aging (grows every second you wait)
const pri = (s, x, now) => (s.vip[x.u] ? VIPB : 0) + (now - x.at) / 1000 * AGE;
const drop = (s, u) => { for (const h in HALLS) { s.act[h] = s.act[h].filter(x => x !== u); s.q[h] = s.q[h].filter(x => x.u !== u); } };

// real mutex: Redis SET NX with expiry, retry until acquired
async function lock() {
  const tok = Math.random().toString(36).slice(2);
  for (let i = 0; i < 50; i++) { if (await r.set('LOCK', tok, { nx: true, px: 3000 })) return tok; await sleep(50); }
  throw new Error('lock busy');
}
const unlock = async tok => { if ((await r.get('LOCK')) === tok) await r.del('LOCK'); };

// writer: exclusive. With locks OFF the read-modify-write races (on purpose, to show double booking)
async function writer(fn, s0) {
  let s = s0, tok = null;
  if (!s.unsafe) { tok = await lock(); s = (await r.get('S')) || fresh(); }
  try {
    const out = await fn(s);
    if (s.unsafe) await sleep(700);
    const p = r.pipeline().set('S', s);   // save and release the lock in one round trip
    if (tok) p.del('LOCK');
    await p.exec(); tok = null;
    return out;
  } finally { if (tok) await unlock(tok); }
}

function clean(s, now, seen) {
  const gone = u => !seen[u] || now - seen[u] > GONE;
  for (const h in HALLS) {
    s.act[h] = s.act[h].filter(u => { if (gone(u)) { lg(s, u + ' disconnected, V(sem) ' + h); return false; } return true; });
    s.q[h] = s.q[h].filter(x => !gone(x.u));
    while (s.act[h].length < CAP && s.q[h].length) {
      s.q[h].sort((a, b) => pri(s, b, now) - pri(s, a, now));
      const x = s.q[h].shift(); s.act[h].push(x.u); lg(s, x.u + ' woken from queue, entered ' + h);
    }
  }
  for (const k in s.holds) {
    const o = s.holds[k];
    if (o.e < now || (!o.p && gone(o.u))) { lg(s, k + ' hold timed out, released'); delete s.holds[k]; }
  }
}

// lazy CPU simulation: replays the scheduler from the last tick up to "now" in 100ms steps
function tick(s, now) {
  const P = s.pay;
  if (!P.q.length) { P.t = now; P.run = null; return; }
  let t = P.t || now;
  while (t < now && P.q.length) {
    let c = P.q.find(j => j.id === P.run);
    if (s.algo === 'prio') { // preemptive priority: VIP first, then arrival
      const b = [...P.q].sort((a, z) => (!!s.vip[z.u] - !!s.vip[a.u]) || a.at - z.at)[0];
      if (c && b.id !== c.id) lg(s, 'PREEMPT: ' + b.u + (s.vip[b.u] ? ' (VIP)' : '') + ' takes CPU from ' + c.u);
      if (!c || b.id !== c.id) P.sl = 0;
      c = b;
    } else if (s.algo === 'rr' && c && P.sl >= QUANTUM) { P.q.push(P.q.splice(P.q.indexOf(c), 1)[0]); c = null; }
    if (!c) { c = s.algo === 'sjf' ? [...P.q].sort((a, z) => a.left - z.left)[0] : P.q[0]; P.sl = 0; }
    P.run = c.id;
    const d = Math.min(100, now - t); c.left -= d; P.sl += d; t += d;
    const l = P.g[P.g.length - 1];
    if (l && l.id === c.id && l.e === t - d) l.e = t; else P.g.push({ id: c.id, u: c.u, s: t - d, e: t });
    if (P.g.length > 24) P.g.shift();
    if (c.left <= 0) {
      P.q.splice(P.q.indexOf(c), 1); P.run = null; P.sl = 0;
      c.seats.forEach(k => { s.booked[k] = c.u; delete s.holds[k]; });
      P.d.push({ w: t - c.at - c.burst, ta: t - c.at }); if (P.d.length > 20) P.d.shift();
      lg(s, 'consumer finished ' + c.u + "'s payment, seats booked"); s.w = { u: c.u, e: now + 2500 };
    }
  }
  P.t = now;
}

function act(s, a, u, b, now) {
  const inH = Object.keys(HALLS).find(h => s.act[h].includes(u));
  if (a === 'enter') {
    const h = b.h;
    if (!HALLS[h] || inH || Object.values(s.q).some(q => q.some(x => x.u === u))) return 'You are already in a hall';
    if (s.act[h].length < CAP) { s.act[h].push(u); lg(s, u + ' wait(sem) ' + h + ', entered'); }
    else { s.q[h].push({ u, at: now }); lg(s, u + ' blocked on ' + h + ' semaphore, queued'); }
  } else if (a === 'leave') {
    drop(s, u); for (const k in s.holds) if (s.holds[k].u === u && !s.holds[k].p) delete s.holds[k]; lg(s, u + ' left, signal(sem)');
  } else if (a === 'hold') {
    const k = String(b.k), h = k.split('-')[0];
    if (!inH || inH !== h) return 'Enter this hall first';
    if (s.booked[k]) return 'Seat already booked';
    const o = s.holds[k];
    if (o && o.u !== u) return 'Seat taken: locked by ' + o.u;
    if (o) { delete s.holds[k]; lg(s, u + ' unlocked ' + k); }
    else {
      if (Object.values(s.holds).filter(x => x.u === u && !x.p).length >= 4) return 'Max 4 seats';
      s.holds[k] = { u, e: now + HOLD }; lg(s, u + ' LOCKED ' + k + ' (30s hold)');
    }
  } else if (a === 'pay') {
    const ks = Object.keys(s.holds).filter(k => s.holds[k].u === u && !s.holds[k].p);
    if (!ks.length) return 'Pick at least one seat first';
    const burst = 2500 + 1500 * ks.length + Math.floor(Math.random() * 1500);
    ks.forEach(k => { s.holds[k].p = 1; s.holds[k].e = now + 36e5; });
    s.pay.q.push({ id: ++s.n, u, seats: ks, burst, left: burst, at: now });
    drop(s, u); lg(s, u + ' produced payment job (' + (burst / 1000).toFixed(1) + 's CPU burst)');
  } else if (a === 'vip') { s.vip[u] = !!b.on; lg(s, u + (b.on ? ' upgraded to VIP' : ' dropped VIP')); }
  else if (a === 'algo' && ['fcfs', 'sjf', 'rr', 'prio'].includes(b.x)) { s.algo = b.x; lg(s, 'scheduler set to ' + b.x.toUpperCase()); }
  else if (a === 'unsafe') { s.unsafe = !!b.on; lg(s, 'locks ' + (b.on ? 'OFF' : 'ON')); }
  else if (a === 'reset') { const v = s.v; for (const k in s) delete s[k]; Object.assign(s, fresh(), { v: v + 1 }); }
  if (a === 'hold' || a === 'pay') s.w = { u, e: now + 2500 };
}

function view(s, u, now, readers) {
  const P = s.pay, av = k => P.d.length ? (P.d.reduce((x, y) => x + y[k], 0) / P.d.length / 1000).toFixed(1) : '-';
  const ord = [...P.q].sort((a, z) => (z.id === P.run) - (a.id === P.run) || (s.algo === 'prio' ? (!!s.vip[z.u] - !!s.vip[a.u] || a.at - z.at) : s.algo === 'sjf' ? a.left - z.left : 0));
  const q = {}; for (const h in HALLS) q[h] = [...s.q[h]].sort((a, z) => pri(s, z, now) - pri(s, a, now)).map(x => ({ u: x.u, vip: !!s.vip[x.u], p: +pri(s, x, now).toFixed(1) }));
  return { now, cap: CAP, halls: HALLS, act: s.act, q, booked: s.booked, holds: s.holds, algo: s.algo, unsafe: s.unsafe, log: s.log, w: s.w, readers, vip: !!s.vip[u],
    pay: { ord: ord.map(j => ({ id: j.id, u: j.u, vip: !!s.vip[j.u], left: j.left, burst: j.burst, n: j.seats.length })), run: P.run, g: P.g, aw: av('w'), at: av('ta') } };
}

module.exports = async (req, res) => {
  try {
    const b = req.body || {}, u = String(b.u || 'anon').slice(0, 12), a = b.a, now = Date.now();
    // one round trip: read state + register presence + fetch who is online
    const [s0, , , z] = await r.pipeline().get('S')
      .zadd('seen', { score: now, member: u }).zremrangebyscore('seen', 0, now - 60000).zrange('seen', 0, -1, { withScores: true }).exec();
    const s = s0 || fresh();
    const seen = {}; for (let i = 0; i < z.length; i += 2) seen[z[i]] = +z[i + 1];
    const readers = Object.values(seen).filter(t => now - t < 5000).length;
    if (a === 'state') { // reader path: no lock, only escalates to the writer if something structural changed
      const v0 = s.v; clean(s, now, seen); tick(s, now);
      if (s.v === v0) return res.json(view(s, u, now, readers));
    }
    const o = await writer(async w => { clean(w, now, seen); tick(w, now); const err = act(w, a, u, b, now); return { err, v: view(w, u, now, readers) }; }, s);
    res.json({ ...o.v, err: o.err });
  } catch (e) { res.status(500).json({ err: String(e.message || e) }); }
};
