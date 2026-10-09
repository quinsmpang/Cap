import { describe, expect, test } from "bun:test";
import {
	mkdirSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	statSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { type FetchStats, type FileSpec, ProjectCache } from "./materialize";

const PIECE = 4 << 20;

function removeFixture(root: string) {
	if (!resolve(root).startsWith(resolve(tmpdir()) + sep)) {
		throw new Error("fixture is outside the temporary directory");
	}
	rmSync(root, { recursive: true, force: true });
}

function file(path: string, size = 128): FileSpec {
	return { path, key: path, size, ranges: "all" };
}

function harness(concurrency = 2) {
	const root = mkdtempSync(join(tmpdir(), "cap-materialize-test-"));
	const reads: { key: string; start: number; end: number }[] = [];
	const gates = new Map<string, Promise<void>>();
	let shortReads = false;
	const cache = new ProjectCache(
		{
			async getRange(key, start, end) {
				reads.push({ key, start, end });
				await gates.get(key);
				return new Uint8Array(end - start + (shortReads ? 0 : 1)).fill(
					Math.floor(start / PIECE) + 1,
				);
			},
		},
		root,
		concurrency,
	);
	return {
		cache,
		root,
		reads,
		gates,
		setShortReads: (value: boolean) => {
			shortReads = value;
		},
		cleanup: () => {
			cache.close();
			removeFixture(root);
		},
	};
}

describe("project materialization", () => {
	test("the default fetch pool admits twelve reads independently of the index pool", async () => {
		const root = mkdtempSync(join(tmpdir(), "cap-materialize-default-"));
		const gate = Promise.withResolvers<void>();
		let started = 0;
		const cache = (() => {
			const configured = process.env.RF_FETCH_CONCURRENCY;
			try {
				delete process.env.RF_FETCH_CONCURRENCY;
				return new ProjectCache(
					{
						async getRange(_key, start, end) {
							started++;
							await gate.promise;
							return new Uint8Array(end - start + 1);
						},
					},
					root,
				);
			} finally {
				if (configured === undefined) delete process.env.RF_FETCH_CONCURRENCY;
				else process.env.RF_FETCH_CONCURRENCY = configured;
			}
		})();
		const request = cache.materialize(
			Array.from({ length: 20 }, (_, i) => file(`asset${i}.bin`)),
		);
		try {
			await Bun.sleep(0);
			expect(started).toBe(12);
		} finally {
			gate.resolve();
			await request;
			cache.close();
			removeFixture(root);
		}
	});

	test("overlapping ranges fetch each piece once and preserve sparse file bytes", async () => {
		const h = harness();
		try {
			const spec: FileSpec = {
				...file("video.mp4", 3 * PIECE + 8),
				ranges: [
					[0, 1],
					[0, 128],
					[3 * PIECE, 3 * PIECE + 8],
				],
			};
			const stats = await h.cache.materialize([spec]);
			expect(stats.requests).toBe(2);
			expect(stats.bytes).toBe(PIECE + 8);
			expect(h.cache.bytesFetched).toBe(PIECE + 8);
			expect(statSync(join(h.root, spec.path)).size).toBe(spec.size);
			const bytes = readFileSync(join(h.root, spec.path));
			expect(bytes[0]).toBe(1);
			expect(bytes[PIECE]).toBe(0);
			expect(bytes[3 * PIECE]).toBe(4);
			expect(bytes[3 * PIECE + 7]).toBe(4);
		} finally {
			h.cleanup();
		}
	});

	test("prefetch and task calls share pending pieces and completed cache hits fetch nothing", async () => {
		const h = harness();
		const gate = Promise.withResolvers<void>();
		h.gates.set("asset.bin", gate.promise);
		try {
			const spec = file("asset.bin");
			const prefetch = h.cache.materialize([spec]);
			const task = h.cache.materialize([spec]);
			gate.resolve();
			const [owner, joiner] = await Promise.all([prefetch, task]);
			expect({ bytes: owner.bytes, requests: owner.requests }).toEqual({
				bytes: 128,
				requests: 1,
			});
			expect({ bytes: joiner.bytes, requests: joiner.requests }).toEqual({
				bytes: 0,
				requests: 0,
			});
			const cached = await h.cache.materialize([spec]);
			expect({ bytes: cached.bytes, requests: cached.requests }).toEqual({
				bytes: 0,
				requests: 0,
			});
			expect(h.reads).toHaveLength(1);
			expect(h.cache.bytesFetched).toBe(128);
		} finally {
			gate.resolve();
			h.cleanup();
		}
	});

	test("short reads are not cached or counted and can be retried", async () => {
		const h = harness();
		try {
			h.setShortReads(true);
			await expect(h.cache.materialize([file("asset.bin")])).rejects.toThrow(
				"short read",
			);
			expect(h.cache.bytesFetched).toBe(0);
			h.setShortReads(false);
			const stats = await h.cache.materialize([file("asset.bin")]);
			expect(stats.requests).toBe(1);
			expect(stats.bytes).toBe(128);
			expect(h.reads).toHaveLength(2);
		} finally {
			h.cleanup();
		}
	});

	test("closing the cache stops late writes and prevents queued downloads from starting", async () => {
		const h = harness(1);
		const gate = Promise.withResolvers<void>();
		h.gates.set("first.bin", gate.promise);
		const work = h.cache.materialize([file("first.bin"), file("second.bin")]);
		try {
			h.cache.close();
			gate.resolve();
			await expect(work).rejects.toThrow("Project cache is closed");
			await Bun.sleep(0);
			expect(h.reads.map((read) => read.key)).toEqual(["first.bin"]);
			expect(h.cache.bytesFetched).toBe(0);
			expect(
				readFileSync(join(h.root, "first.bin")).every((value) => value === 0),
			).toBe(true);
			await expect(h.cache.materialize([])).rejects.toThrow(
				"Project cache is closed",
			);
		} finally {
			gate.resolve();
			await Promise.allSettled([work]);
			h.cleanup();
		}
	});

	test("path aliases reuse the canonical file without truncating downloaded data", async () => {
		const h = harness();
		try {
			await h.cache.materialize([file("asset.bin")]);
			const stats = await h.cache.materialize([
				file("./asset.bin"),
				file("nested/../asset.bin"),
			]);
			expect(stats.requests).toBe(0);
			expect(h.reads).toHaveLength(1);
			expect(
				readFileSync(join(h.root, "asset.bin")).every((value) => value === 1),
			).toBe(true);
			await expect(
				h.cache.materialize([file("../escape.bin")]),
			).rejects.toThrow("outside the project");
		} finally {
			h.cleanup();
		}
	});

	test("relative cache roots keep their existing behavior after a working-directory change", async () => {
		const originalDirectory = process.cwd();
		const root = mkdtempSync(join(tmpdir(), "cap-materialize-relative-"));
		let cache: ProjectCache | undefined;
		let reads = 0;
		try {
			for (const name of ["first", "second"]) mkdirSync(join(root, name));
			process.chdir(join(root, "first"));
			cache = new ProjectCache(
				{
					async getRange(_key, start, end) {
						return new Uint8Array(end - start + 1).fill(++reads);
					},
				},
				"project",
			);
			await cache.materialize([file("asset.bin")]);
			process.chdir(join(root, "second"));
			await cache.materialize([file("asset.bin")]);
			expect(reads).toBe(2);
			expect(readFileSync(join(root, "first/project/asset.bin"))[0]).toBe(1);
			expect(readFileSync(join(root, "second/project/asset.bin"))[0]).toBe(2);
		} finally {
			process.chdir(originalDirectory);
			cache?.close();
			removeFixture(root);
		}
	});

	for (const concurrency of [
		0,
		-1,
		Number.NaN,
		Number.POSITIVE_INFINITY,
		0.5,
	]) {
		test(`invalid or fractional concurrency cannot strand downloads: ${concurrency}`, async () => {
			const h = harness(concurrency);
			try {
				expect((await h.cache.materialize([file("asset.bin")])).requests).toBe(
					1,
				);
			} finally {
				h.cleanup();
			}
		});
	}

	test("project configuration is rewritten after download and only once", async () => {
		const root = mkdtempSync(join(tmpdir(), "cap-materialize-config-"));
		const bytes = new TextEncoder().encode(
			'{"asset":"$RF_PROJECT/wallpaper.png"}',
		);
		let reads = 0;
		const cache = new ProjectCache(
			{
				async getRange(_key, start, end) {
					reads++;
					return bytes.slice(start, end + 1);
				},
			},
			root,
		);
		try {
			const spec = file("project-config.json", bytes.byteLength);
			await cache.materialize([spec]);
			expect(readFileSync(join(root, spec.path), "utf8")).toBe(
				`{"asset":"${root}/wallpaper.png"}`,
			);
			writeFileSync(join(root, spec.path), "edited $RF_PROJECT");
			await cache.materialize([spec]);
			expect(readFileSync(join(root, spec.path), "utf8")).toBe(
				"edited $RF_PROJECT",
			);
			expect(reads).toBe(1);
		} finally {
			cache.close();
			removeFixture(root);
		}
	});

	test("requests racing with a released slot cannot overtake queued downloads", async () => {
		const root = mkdtempSync(join(tmpdir(), "cap-materialize-race-"));
		const firstGate = Promise.withResolvers<void>();
		const othersGate = Promise.withResolvers<void>();
		const raced = Promise.withResolvers<FetchStats>();
		let active = 0;
		let peak = 0;
		const cache = new ProjectCache(
			{
				async getRange(key, start, end) {
					active++;
					peak = Math.max(peak, active);
					if (key === "first.bin") {
						await firstGate.promise;
						queueMicrotask(() =>
							queueMicrotask(() =>
								queueMicrotask(() => {
									cache
										.materialize([file("third.bin")])
										.then(raced.resolve, raced.reject);
								}),
							),
						);
					} else {
						await othersGate.promise;
					}
					active--;
					return new Uint8Array(end - start + 1);
				},
			},
			root,
			1,
		);
		const first = cache.materialize([file("first.bin")]);
		const second = cache.materialize([file("second.bin")]);
		try {
			firstGate.resolve();
			await first;
			await Bun.sleep(0);
			expect(peak).toBe(1);
		} finally {
			firstGate.resolve();
			othersGate.resolve();
			await Promise.allSettled([first, second, raced.promise]);
			cache.close();
			removeFixture(root);
		}
	});
});
