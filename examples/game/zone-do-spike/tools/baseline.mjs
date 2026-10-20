// Noise floor: setTimeout 30 Hz loop lateness in a plain Node process.
const hz = Number(process.argv[2] ?? 30), secs = Number(process.argv[3] ?? 15), step = 1000 / hz;
const start = performance.now(); const late = []; let k = 1;
function tick() { const now = performance.now(); late.push(now - (start + (k - 1) * step)); k = Math.floor((now - start) / step) + 2; if (now - start < secs * 1000) setTimeout(tick, Math.max(0, start + (k - 1) * step - performance.now())); else { late.shift(); const s = late.sort((a, b) => a - b); console.log(JSON.stringify({ baselineNodeTimer: { hz, p50: +s[Math.floor(s.length / 2)].toFixed(2), p99: +s[Math.floor(s.length * 0.99)].toFixed(2), max: +s.at(-1).toFixed(2) } })); } }
setTimeout(tick, step);
