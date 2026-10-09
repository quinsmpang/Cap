import { describe, expect, test } from "bun:test";
import { TranscodeInputs } from "./transcode-inputs";

describe("transcode inputs", () => {
	test("concurrent callers share pending reads, then observe changed objects", async () => {
		const gate = Promise.withResolvers<void>();
		let size = 10;
		let reads = 0;
		const inputs = new TranscodeInputs(
			{
				head: async () => {
					reads++;
					await gate.promise;
					return { size };
				},
			},
			100,
		);
		const requests = Array.from({ length: 20 }, (_, index) =>
			index % 2 ? inputs.sourceSize("shared") : inputs.storedSize("shared"),
		);
		expect(reads).toBe(1);
		gate.resolve();
		expect(await Promise.all(requests)).toEqual(Array(20).fill(10));
		size = 11;
		expect(await inputs.sourceSize("shared")).toBe(11);
		expect(reads).toBe(2);
	});

	test("failed shared reads are removed before retry", async () => {
		let reads = 0;
		const inputs = new TranscodeInputs(
			{
				head: async () => {
					if (++reads === 1) throw new Error("storage failure");
					return { size: 1 };
				},
			},
			100,
		);
		const results = await Promise.allSettled(
			Array.from({ length: 10 }, () => inputs.sourceSize("shared")),
		);
		expect(results.every((result) => result.status === "rejected")).toBe(true);
		expect(reads).toBe(1);
		expect(await inputs.sourceSize("shared")).toBe(1);
		expect(reads).toBe(2);
	});

	test("different keys respect the shared limit and errors release slots", async () => {
		const gate = Promise.withResolvers<void>();
		let active = 0;
		let peak = 0;
		const reads: string[] = [];
		const inputs = new TranscodeInputs(
			{
				head: async (key) => {
					reads.push(key);
					peak = Math.max(peak, ++active);
					try {
						await gate.promise;
						if (key === "0") throw new Error("failed");
						return { size: 1 };
					} finally {
						active--;
					}
				},
			},
			100,
			2,
		);
		const requests = Array.from({ length: 20 }, (_, index) =>
			inputs.sourceSize(String(index)),
		);
		const completed = Promise.allSettled(requests);
		try {
			expect(reads).toEqual(["0", "1"]);
		} finally {
			gate.resolve();
		}
		const results = await completed;
		expect(peak).toBe(2);
		expect(active).toBe(0);
		expect(
			results.filter((result) => result.status === "fulfilled"),
		).toHaveLength(19);
		expect(reads).toEqual(
			Array.from({ length: 20 }, (_, index) => String(index)),
		);
	});

	for (const size of [
		0,
		-1,
		0.5,
		NaN,
		Infinity,
		Number.MAX_SAFE_INTEGER + 1,
		101,
	]) {
		test(`size ${size} retains validation for both uses`, async () => {
			const inputs = new TranscodeInputs({ head: async () => ({ size }) }, 100);
			await expect(inputs.storedSize("output")).rejects.toThrow(
				"stored transcode output has an invalid size",
			);
			await expect(inputs.sourceSize("source")).rejects.toThrow(
				size === 101
					? "101 bytes (limit 100)"
					: "missing or has an invalid size",
			);
		});
	}

	test("missing outputs are reusable checks, while missing raw sources fail", async () => {
		let size: number | null = null;
		const inputs = new TranscodeInputs(
			{ head: async () => (size === null ? null : { size }) },
			100,
		);
		expect(await inputs.storedSize("key")).toBeNull();
		await expect(inputs.sourceSize("key")).rejects.toThrow(
			"missing or has an invalid size",
		);
		size = 100;
		expect(await inputs.storedSize("key")).toBe(100);
		expect(await inputs.sourceSize("key")).toBe(100);
	});
});
