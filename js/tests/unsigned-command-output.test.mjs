import assert from 'node:assert/strict';
import test from 'node:test';
import { cloneOutputScalar } from '../dist/replica/command-runtime/lib/output.js';

test('unsigned command results reject lossy numbers before entering the replica', () => {
    for (const [codec, maximum] of [['uint8', 255], ['uint16', 65_535],
        ['uint32', 4_294_967_295], ['uint64_safe_integer', Number.MAX_SAFE_INTEGER]]) {
        assert.equal(cloneOutputScalar(codec, maximum, 'result.revision'), maximum);
        for (const value of [-1, -0, 0.5, NaN, Infinity, maximum + 1, 2 ** 64, '1']) {
            assert.throws(() => cloneOutputScalar(codec, value, 'result.revision'));
        }
    }
    assert.equal(cloneOutputScalar('json_number_precision_limited', -1, 'signed'), -1);
    assert.equal(cloneOutputScalar('int32', -2_147_483_648, 'signed'), -2_147_483_648);
});
