import { describe, expect, test } from "bun:test";
import { ConcurrencyLimiter } from "./concurrency-limiter";

describe("concurrency limiter", () => {
	test("queued reads keep FIFO order and stay within capacity", async () => {
		const limiter = new ConcurrencyLimiter(2);
		const gate = Promise.withResolvers<void>();
		const order: number[] = [];
		let active = 0;
		let peak = 0;
		const requests = Array.from({ length: 128 }, (_, i) =>
			limiter.run(async () => {
				active++;
				peak = Math.max(peak, active);
				order.push(i);
				if (i < 2) await gate.promise;
				else await Promise.resolve();
				active--;
				return i;
			}),
		);
		gate.resolve();
		expect(await Promise.all(requests)).toEqual(
			Array.from({ length: 128 }, (_, i) => i),
		);
		expect(order).toEqual(Array.from({ length: 128 }, (_, i) => i));
		expect(peak).toBe(2);
	});

	test("synchronous throws and asynchronous failures release their slots", async () => {
		const limiter = new ConcurrencyLimiter(1);
		const gate = Promise.withResolvers<void>();
		const requests = [
			limiter.run(async () => {
				await gate.promise;
				throw new Error("async failure");
			}),
			limiter.run(() => {
				throw new Error("sync failure");
			}),
			limiter.run(async () => "done"),
		];
		const settled = Promise.allSettled(requests);
		gate.resolve();
		expect((await settled).map((result) => result.status)).toEqual([
			"rejected",
			"rejected",
			"fulfilled",
		]);
		expect(await requests[2]).toBe("done");
		expect(await limiter.run(async () => "reused")).toBe("reused");
	});

	test("a fresh request cannot take a slot already handed to a waiter", async () => {
		const limiter = new ConcurrencyLimiter(1);
		const gate = Promise.withResolvers<void>();
		const raced = Promise.withResolvers<void>();
		const order: string[] = [];
		const first = limiter.run(async () => {
			await gate.promise;
			queueMicrotask(() =>
				queueMicrotask(() => {
					limiter
						.run(async () => {
							order.push("new");
						})
						.then(raced.resolve, raced.reject);
				}),
			);
		});
		const second = limiter.run(async () => {
			order.push("waiting");
		});
		gate.resolve();
		await Promise.all([first, second, raced.promise]);
		expect(order).toEqual(["waiting", "new"]);
	});

	test("large queues preserve order across compaction, emptying and reuse", async () => {
		const limiter = new ConcurrencyLimiter(3);
		for (let round = 0; round < 3; round++) {
			const order: number[] = [];
			const expected = Array.from({ length: 10000 }, (_, i) => i);
			const result = await Promise.all(
				expected.map((i) =>
					limiter.run(async () => {
						order.push(i);
						await Promise.resolve();
						return i;
					}),
				),
			);
			expect(result).toEqual(expected);
			expect(order).toEqual(expected);
			expect(await limiter.run(async () => round)).toBe(round);
		}
	});

	test("separate instances do not share a concurrency budget", async () => {
		const first = new ConcurrencyLimiter(1);
		const second = new ConcurrencyLimiter(1);
		const gate = Promise.withResolvers<void>();
		const blocked = first.run(() => gate.promise);
		try {
			expect(await second.run(async () => "independent")).toBe("independent");
		} finally {
			gate.resolve();
			await blocked;
		}
	});

	for (const limit of [0, -1, 0.5, Number.NaN, Number.POSITIVE_INFINITY]) {
		test(`non-positive or non-integer capacities are rejected: ${limit}`, () => {
			expect(() => new ConcurrencyLimiter(limit)).toThrow("positive integer");
		});
	}
});
