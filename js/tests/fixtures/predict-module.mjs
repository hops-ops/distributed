/**
 * Hand-assembled WebAssembly modules speaking the prediction ABI
 * (`distributed_game::prediction`, v1), for SDK tests that need no Rust
 * toolchain. Steps:
 *
 * - `dg_predict_<echo>`: returns `state_len` (the next state is the state);
 * - `dg_predict_fail`: returns -3 (input rejected).
 */
function leb(value) {
	const out = [];
	let v = value >>> 0;
	do {
		let byte = v & 0x7f;
		v >>>= 7;
		if (v !== 0) byte |= 0x80;
		out.push(byte);
	} while (v !== 0);
	return out;
}

function str(s) {
	const bytes = [...new TextEncoder().encode(s)];
	return [...leb(bytes.length), ...bytes];
}

function section(id, body) {
	return [id, ...leb(body.length), ...body];
}

/** Bytes of a prediction module (`abi` is what `dg_predict_abi` returns). */
export function predictModuleBytes({ echo = 'sample_step', abi = 1, buffer = 1024 } = {}) {
	const types = [2, 0x60, 0, 1, 0x7f, 0x60, 4, 0x7f, 0x7f, 0x7f, 0x7d, 1, 0x7f];
	const functions = [4, 0, 0, 1, 1];
	const memory = [1, 0, 2]; // 2 pages: buffer + 64 KiB scratch
	const exports = [
		5,
		...str('memory'), 2, 0,
		...str('dg_predict_abi'), 0, 0,
		...str('dg_predict_buffer'), 0, 1,
		...str(`dg_predict_${echo}`), 0, 2,
		...str('dg_predict_fail'), 0, 3
	];
	// Signed LEB128 for i32.const (small values only).
	const sleb = (v) => {
		const out = [];
		let more = true;
		while (more) {
			let byte = v & 0x7f;
			v >>= 7;
			if ((v === 0 && (byte & 0x40) === 0) || (v === -1 && (byte & 0x40) !== 0)) more = false;
			else byte |= 0x80;
			out.push(byte);
		}
		return out;
	};
	const body = (code) => [...leb(code.length + 1), 0, ...code];
	const code = [
		4,
		...body([0x41, ...sleb(abi), 0x0b]),
		...body([0x41, ...sleb(buffer), 0x0b]),
		...body([0x20, 0, 0x0b]),
		...body([0x41, ...sleb(-3), 0x0b])
	];
	return new Uint8Array([
		0x00, 0x61, 0x73, 0x6d, 1, 0, 0, 0,
		...section(1, types),
		...section(3, functions),
		...section(5, memory),
		...section(7, exports),
		...section(10, code)
	]);
}
