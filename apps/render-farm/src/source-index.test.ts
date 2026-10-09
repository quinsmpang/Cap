import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { ANNEX_B_PARAMETER_SETS } from "./boxes.test-util";
import { avcC, buildHeader, locateMoov } from "./mp4";
import { type Manifest, SourceIndexes } from "./source-index";

function harness(env: Record<string, string> = {}) {
	const objects = new Map<string, Uint8Array>();
	const reads: string[] = [];
	const ranges: { key: string; start: number; end: number }[] = [];
	const gates = new Map<string, Promise<void>>();
	const rangeGates = new Map<string, Promise<void>>();
	const failures = new Set<string>();
	const transcodes: ConstructorParameters<typeof SourceIndexes>[1] = {
		storedSize: async () => null,
		sourceSize: async () => 1,
		run: async () => 1,
	};
	let tagged = true;
	const storage = {
		async getRange(key: string, start: number, end: number) {
			reads.push(key);
			ranges.push({ key, start, end });
			const bytes = objects.get(key);
			await gates.get(key);
			await rangeGates.get(`${key}:${start}`);
			if (failures.has(key)) throw new Error(`read failed: ${key}`);
			if (!bytes) throw new Error(`missing: ${key}`);
			return bytes.slice(start, end + 1);
		},
		async getRangeTagged(key: string, start: number, end: number) {
			const etag = tagged
				? createHash("md5")
						.update(objects.get(key) ?? new Uint8Array())
						.digest("hex")
				: null;
			const bytes = await this.getRange(key, start, end);
			return { bytes, etag };
		},
	};
	const indexes = new SourceIndexes(storage, transcodes, env);
	const recording = (prefix: string, files: Manifest["files"] = []) => {
		objects.set(
			`${prefix}/recording-meta.json`,
			new TextEncoder().encode("{}"),
		);
		objects.set(
			`${prefix}/manifest.json`,
			new TextEncoder().encode(
				JSON.stringify({
					files: [{ path: "recording-meta.json", size: 2 }, ...files],
				}),
			),
		);
	};
	return {
		indexes,
		objects,
		reads,
		ranges,
		gates,
		rangeGates,
		failures,
		transcodes,
		recording,
		untagged: () => {
			tagged = false;
		},
	};
}

function video(sampleSize = 1) {
	const header = buildHeader({
		width: 128,
		height: 72,
		fps: 30,
		video: {
			sizes: new Uint32Array(3).fill(sampleSize),
			runs: [{ first: 0, count: 3, offset: 0 }],
			keyframes: Uint32Array.of(0),
			avcC: avcC(ANNEX_B_PARAMETER_SETS),
		},
		audio: null,
		payloadSize: 3 * sampleSize,
		minimumSize: 0,
	});
	const location = locateMoov(header, header.byteLength);
	if (!location || !("start" in location) || location.start === undefined) {
		throw new Error("fixture has no moov");
	}
	const mdat = 2 * 1024 * 1024;
	const bytes = new Uint8Array(mdat + location.size);
	new DataView(bytes.buffer).setUint32(0, mdat);
	bytes.set(new TextEncoder().encode("mdat"), 4);
	bytes.set(
		header.subarray(location.start, location.start + location.size),
		mdat,
	);
	return bytes;
}

describe("source indexes", () => {
	for (const [configured, capacity] of [
		[undefined, 16],
		["3", 3],
		["0", 16],
		["-1", 1],
		["0.5", 16],
		["NaN", 16],
		["Infinity", 16],
	] as const) {
		test(`video read capacity remains bounded with configuration ${configured}`, async () => {
			const h = harness(
				configured === undefined
					? {}
					: { RF_INDEX_READ_CONCURRENCY: configured },
			);
			const bytes = video();
			const gate = Promise.withResolvers<void>();
			const files = Array.from({ length: 20 }, (_, i) => {
				const path = `video${i}.mp4`;
				h.objects.set(`recording/${path}`, bytes);
				h.gates.set(`recording/${path}`, gate.promise);
				return { path, size: bytes.byteLength };
			});
			h.recording("recording", files);
			const request = h.indexes.get("recording");
			try {
				await Bun.sleep(0);
				expect(
					h.ranges.filter(
						(read) => read.key.endsWith(".mp4") && read.start === 0,
					),
				).toHaveLength(capacity);
			} finally {
				gate.resolve();
				await request;
			}
		});
	}

	test("concurrent requests share reads and deliver each manifest before completion", async () => {
		const h = harness();
		h.recording("recording");
		const gate = Promise.withResolvers<void>();
		h.gates.set("recording/recording-meta.json", gate.promise);
		const notified = Promise.withResolvers<void>();
		let callbacks = 0;
		const requests = Array.from({ length: 10 }, () =>
			h.indexes.get("recording", undefined, () => {
				if (++callbacks === 10) notified.resolve();
			}),
		);
		await notified.promise;
		const lateNotified = Promise.withResolvers<void>();
		let completed = false;
		const late = h.indexes
			.get("recording", undefined, () => lateNotified.resolve())
			.then((index) => {
				completed = true;
				return index;
			});
		await lateNotified.promise;
		expect(completed).toBe(false);
		gate.resolve();
		const results = await Promise.all([...requests, late]);
		expect(new Set(results).size).toBe(1);
		expect(h.reads).toEqual([
			"recording/manifest.json",
			"recording/recording-meta.json",
		]);
		let cachedCallback = false;
		expect(
			await h.indexes.get("recording", undefined, () => {
				cachedCallback = true;
			}),
		).toBe(results[0]);
		expect(cachedCallback).toBe(true);
		expect(h.reads).toHaveLength(2);
	});

	test("a callback failure does not poison another request or its cached result", async () => {
		const h = harness();
		h.recording("recording");
		const failed = h.indexes.get("recording", undefined, () => {
			throw new Error("callback failed");
		});
		const successful = h.indexes.get("recording");
		await expect(failed).rejects.toThrow("callback failed");
		const index = await successful;
		expect(await h.indexes.get("recording")).toBe(index);
		expect(h.reads).toHaveLength(2);
	});

	test("manifest callbacks do not make indexing wait for their returned promises", async () => {
		const h = harness();
		h.recording("recording");
		const gate = Promise.withResolvers<void>();
		const index = await h.indexes.get(
			"recording",
			undefined,
			() => gate.promise,
		);
		expect(index.recordingMeta).toEqual({});
		gate.resolve();
	});

	test("a failed build remains handled after its only callback has failed", async () => {
		const h = harness();
		h.recording("recording");
		const gate = Promise.withResolvers<void>();
		h.gates.set("recording/recording-meta.json", gate.promise);
		h.failures.add("recording/recording-meta.json");
		await expect(
			h.indexes.get("recording", undefined, () => {
				throw new Error("callback failed");
			}),
		).rejects.toThrow("callback failed");
		gate.resolve();
		await Bun.sleep(0);
		h.failures.clear();
		expect((await h.indexes.get("recording")).recordingMeta).toEqual({});
	});

	for (const key of ["manifest.json", "recording-meta.json", "display.mp4"]) {
		test(`failed ${key} reads can be retried`, async () => {
			const h = harness();
			const bytes = video();
			h.recording("recording", [
				{ path: "display.mp4", size: bytes.byteLength },
			]);
			h.objects.set("recording/display.mp4", bytes);
			h.failures.add(`recording/${key}`);
			const results = await Promise.allSettled([
				h.indexes.get("recording"),
				h.indexes.get("recording"),
			]);
			expect(results.map((result) => result.status)).toEqual([
				"rejected",
				"rejected",
			]);
			h.failures.clear();
			expect((await h.indexes.get("recording")).mediaMeta.size).toBe(1);
		});
	}

	for (const narrowFirst of [true, false]) {
		test(`concurrent source scopes stay isolated, narrow first: ${narrowFirst}`, async () => {
			const h = harness();
			const prefix = "owner/video/project";
			h.recording(prefix, [
				{ path: "shared.json", key: "owner/other/shared.json", size: 2 },
			]);
			let deniedCallback = false;
			const broad = () => h.indexes.get(prefix, "owner/");
			const narrow = () =>
				h.indexes.get(prefix, "owner/video/", () => {
					deniedCallback = true;
				});
			const requests = narrowFirst ? [narrow(), broad()] : [broad(), narrow()];
			const results = await Promise.allSettled(requests);
			expect(results.map((result) => result.status)).toEqual(
				narrowFirst ? ["rejected", "fulfilled"] : ["fulfilled", "rejected"],
			);
			expect(deniedCallback).toBe(false);
			await expect(h.indexes.get(prefix, "owner/video/")).rejects.toThrow(
				"outside the recording",
			);
			await expect(h.indexes.get(prefix)).rejects.toThrow(
				"outside the recording",
			);
		});
	}

	test("disabling the cache keeps concurrent and sequential requests independent", async () => {
		const h = harness({ RF_INDEX_CACHE: "0" });
		h.recording("recording");
		await Promise.all(
			Array.from({ length: 10 }, () => h.indexes.get("recording")),
		);
		expect(h.reads).toHaveLength(20);
		await h.indexes.get("recording");
		expect(h.reads).toHaveLength(22);
	});

	test("unchanged sources reuse moov across projects and same-size replacements refresh it", async () => {
		const h = harness();
		const key = "owner/video/display.mp4";
		const first = video();
		const replacement = video(2);
		expect(replacement.byteLength).toBe(first.byteLength);
		const load = async (prefix: string, bytes: Uint8Array) => {
			h.objects.set(key, bytes);
			h.recording(prefix, [
				{ path: "display.mp4", key, size: bytes.byteLength },
			]);
			const before = h.reads.filter((read) => read === key).length;
			const index = await h.indexes.get(prefix, "owner/video/");
			return {
				reads: h.reads.filter((read) => read === key).length - before,
				sampleSize: index.mediaMeta.get("display.mp4")?.index.sizes[0],
			};
		};
		expect(await load("owner/video/project1", first)).toEqual({
			reads: 2,
			sampleSize: 1,
		});
		expect(await load("owner/video/project2", first)).toEqual({
			reads: 1,
			sampleSize: 1,
		});
		expect(await load("owner/video/project3", replacement)).toEqual({
			reads: 2,
			sampleSize: 2,
		});
		h.untagged();
		expect(await load("owner/video/project4", replacement)).toEqual({
			reads: 2,
			sampleSize: 2,
		});
	});

	test("concurrent projects share video indexing after checking each source version", async () => {
		const h = harness();
		const key = "owner/video/display.mp4";
		const bytes = video();
		h.objects.set(key, bytes);
		const requests = Array.from({ length: 10 }, (_, i) => {
			const prefix = `owner/video/project${i}`;
			h.recording(prefix, [
				{ path: "display.mp4", key, size: bytes.byteLength },
			]);
			return h.indexes.get(prefix, "owner/video/");
		});
		const indexes = await Promise.all(requests);
		expect({
			headReads: h.ranges.filter((read) => read.key === key && read.start === 0)
				.length,
			tailReads: h.ranges.filter((read) => read.key === key && read.start > 0)
				.length,
			parsedIndexes: new Set(
				indexes.map((index) => index.mediaMeta.get("display.mp4")?.index),
			).size,
		}).toEqual({ headReads: 10, tailReads: 1, parsedIndexes: 1 });
	});

	test("waiting on a shared index leaves a reader slot for unrelated sources", async () => {
		const h = harness({ RF_INDEX_READ_CONCURRENCY: "2" });
		const bytes = video();
		const shared = "owner/video/shared.mp4";
		const unrelated = "owner/video/unrelated.mp4";
		h.objects.set(shared, bytes);
		h.objects.set(unrelated, bytes);
		for (const [prefix, key] of [
			["first", shared],
			["second", shared],
			["third", unrelated],
		] as const) {
			h.recording(`owner/video/${prefix}`, [
				{ path: "display.mp4", key, size: bytes.byteLength },
			]);
		}
		const gate = Promise.withResolvers<void>();
		h.rangeGates.set(`${shared}:${2 * 1024 * 1024}`, gate.promise);
		const first = h.indexes.get("owner/video/first", "owner/video/");
		try {
			await Bun.sleep(0);
			const second = h.indexes.get("owner/video/second", "owner/video/");
			await Bun.sleep(0);
			const third = await h.indexes.get("owner/video/third", "owner/video/");
			expect(third.mediaMeta.get("display.mp4")?.key).toBe(unrelated);
			expect(
				h.ranges.filter((read) => read.key === shared && read.start > 0),
			).toHaveLength(1);
			gate.resolve();
			await Promise.all([first, second]);
		} finally {
			gate.resolve();
		}
	});

	test("an owning reader finishes its source before queued source heads begin", async () => {
		const h = harness({ RF_INDEX_READ_CONCURRENCY: "1" });
		const bytes = video();
		h.recording(
			"recording",
			["first.mp4", "second.mp4"].map((path) => ({
				path,
				size: bytes.byteLength,
			})),
		);
		for (const path of ["first.mp4", "second.mp4"]) {
			h.objects.set(`recording/${path}`, bytes);
		}
		await h.indexes.get("recording");
		expect(
			h.ranges
				.filter((read) => read.key.endsWith(".mp4"))
				.map(({ key, start }) => ({ key, start })),
		).toEqual([
			{ key: "recording/first.mp4", start: 0 },
			{ key: "recording/first.mp4", start: 2 * 1024 * 1024 },
			{ key: "recording/second.mp4", start: 0 },
			{ key: "recording/second.mp4", start: 2 * 1024 * 1024 },
		]);
	});

	test("a same-size replacement is indexed independently of an older pending version", async () => {
		const h = harness();
		const key = "owner/video/display.mp4";
		const bytes = video();
		const replacement = video(2);
		expect(replacement.byteLength).toBe(bytes.byteLength);
		h.objects.set(key, bytes);
		for (const prefix of ["first", "second"]) {
			h.recording(`owner/video/${prefix}`, [
				{ path: "display.mp4", key, size: bytes.byteLength },
			]);
		}
		const gate = Promise.withResolvers<void>();
		const tailKey = `${key}:${2 * 1024 * 1024}`;
		h.rangeGates.set(tailKey, gate.promise);
		const first = h.indexes.get("owner/video/first", "owner/video/");
		try {
			await Bun.sleep(0);
			expect(h.ranges.some((read) => read.key === key && read.start > 0)).toBe(
				true,
			);
			h.objects.set(key, replacement);
			h.rangeGates.delete(tailKey);
			const second = await h.indexes.get("owner/video/second", "owner/video/");
			expect(second.mediaMeta.get("display.mp4")?.index.sizes[0]).toBe(2);
			gate.resolve();
			expect((await first).mediaMeta.get("display.mp4")?.index.sizes[0]).toBe(
				1,
			);
		} finally {
			gate.resolve();
		}
	});

	for (const mode of ["missing-etag", "disabled-cache"]) {
		test(`video reads remain independent with ${mode}`, async () => {
			const h = harness(
				mode === "disabled-cache" ? { RF_INDEX_CACHE: "0" } : {},
			);
			if (mode === "missing-etag") h.untagged();
			const key = "owner/video/display.mp4";
			const bytes = video();
			h.objects.set(key, bytes);
			await Promise.all(
				Array.from({ length: 3 }, (_, i) => {
					const prefix = `owner/video/project${i}`;
					h.recording(prefix, [
						{ path: "display.mp4", key, size: bytes.byteLength },
					]);
					return h.indexes.get(prefix, "owner/video/");
				}),
			);
			expect(
				h.ranges.filter((read) => read.key === key && read.start > 0),
			).toHaveLength(3);
		});
	}

	test("a failed shared tail rejects its waiters and is read again on retry", async () => {
		const h = harness();
		const key = "owner/video/display.mp4";
		const bytes = video();
		h.objects.set(key, bytes);
		for (const prefix of ["first", "second", "retry"]) {
			h.recording(`owner/video/${prefix}`, [
				{ path: "display.mp4", key, size: bytes.byteLength },
			]);
		}
		const gate = Promise.withResolvers<void>();
		h.rangeGates.set(`${key}:${2 * 1024 * 1024}`, gate.promise);
		const results = Promise.allSettled([
			h.indexes.get("owner/video/first", "owner/video/"),
			h.indexes.get("owner/video/second", "owner/video/"),
		]);
		await Bun.sleep(0);
		expect(
			h.ranges.filter((read) => read.key === key && read.start === 0),
		).toHaveLength(2);
		h.failures.add(key);
		gate.resolve();
		expect((await results).map((result) => result.status)).toEqual([
			"rejected",
			"rejected",
		]);
		h.failures.clear();
		expect(
			(await h.indexes.get("owner/video/retry", "owner/video/")).mediaMeta.size,
		).toBe(1);
		expect(
			h.ranges.filter((read) => read.key === key && read.start > 0),
		).toHaveLength(2);
	});

	test("a failed version check cannot borrow another project's pending index", async () => {
		const h = harness();
		const key = "owner/video/display.mp4";
		const bytes = video();
		h.objects.set(key, bytes);
		for (const prefix of ["first", "second"]) {
			h.recording(`owner/video/${prefix}`, [
				{ path: "display.mp4", key, size: bytes.byteLength },
			]);
		}
		const gate = Promise.withResolvers<void>();
		h.rangeGates.set(`${key}:${2 * 1024 * 1024}`, gate.promise);
		const first = h.indexes.get("owner/video/first", "owner/video/");
		try {
			await Bun.sleep(0);
			h.failures.add(key);
			await expect(
				h.indexes.get("owner/video/second", "owner/video/"),
			).rejects.toThrow("read failed");
			h.failures.clear();
			gate.resolve();
			expect((await first).mediaMeta.size).toBe(1);
		} finally {
			h.failures.clear();
			gate.resolve();
		}
	});

	test("an out-of-scope project cannot borrow a validated pending video index", async () => {
		const h = harness();
		const key = "owner/video/display.mp4";
		const bytes = video();
		h.objects.set(key, bytes);
		for (const prefix of ["owner/video/project", "owner/other/project"]) {
			h.recording(prefix, [
				{ path: "display.mp4", key, size: bytes.byteLength },
			]);
		}
		const gate = Promise.withResolvers<void>();
		h.rangeGates.set(`${key}:${2 * 1024 * 1024}`, gate.promise);
		const valid = h.indexes.get("owner/video/project", "owner/video/");
		try {
			await Bun.sleep(0);
			await expect(
				h.indexes.get("owner/other/project", "owner/other/"),
			).rejects.toThrow("outside the recording");
			expect(
				h.ranges.filter((read) => read.key === key && read.start === 0),
			).toHaveLength(1);
			gate.resolve();
			await valid;
		} finally {
			gate.resolve();
		}
	});

	test("manifest delivery precedes transcode preflight and raw quotas precede scheduling", async () => {
		const h = harness({ RF_MAX_SOURCE_BYTES: "10" });
		h.recording("recording", [
			{
				path: "display.mp4",
				key: "recording/display.mp4",
				transcodeFrom: "recording/raw.webm",
				size: 0,
			},
		]);
		let notified = false;
		let scheduled = 0;
		h.transcodes.sourceSize = async () => {
			expect(notified).toBe(true);
			return 20;
		};
		h.transcodes.run = async () => {
			scheduled++;
			return 1;
		};
		await expect(
			h.indexes.get("recording", undefined, () => {
				notified = true;
			}),
		).rejects.toThrow("limit 10");
		expect(scheduled).toBe(0);
	});

	test("failed transcodes retry and their output must still satisfy quotas", async () => {
		const h = harness({ RF_MAX_SOURCE_BYTES: "10" });
		h.recording("recording", [
			{
				path: "display.mp4",
				key: "recording/display.mp4",
				transcodeFrom: "recording/raw.webm",
				size: 0,
			},
		]);
		h.transcodes.run = async () => {
			throw new Error("transcode failed");
		};
		await expect(h.indexes.get("recording")).rejects.toThrow(
			"transcode failed",
		);
		h.transcodes.run = async () => 20;
		await expect(h.indexes.get("recording")).rejects.toThrow("limit 10");
	});

	test("completed recordings retain the existing 32-entry eviction order", async () => {
		const h = harness();
		for (let i = 0; i < 33; i++) {
			h.recording(`recording${i}`);
			await h.indexes.get(`recording${i}`);
		}
		const before = h.reads.length;
		await h.indexes.get("recording32");
		expect(h.reads).toHaveLength(before);
		await h.indexes.get("recording0");
		expect(h.reads).toHaveLength(before + 2);
	});

	test("video reads share a limit across recordings and failure releases the slot", async () => {
		const h = harness({ RF_INDEX_READ_CONCURRENCY: "1" });
		const bytes = video();
		for (const prefix of ["first", "second"]) {
			h.recording(prefix, [{ path: "display.mp4", size: bytes.byteLength }]);
			h.objects.set(`${prefix}/display.mp4`, bytes);
		}
		const gate = Promise.withResolvers<void>();
		h.gates.set("first/display.mp4", gate.promise);
		h.failures.add("first/display.mp4");
		const results = Promise.allSettled([
			h.indexes.get("first"),
			h.indexes.get("second"),
		]);
		await Bun.sleep(0);
		expect(h.reads).toContain("first/display.mp4");
		expect(h.reads).not.toContain("second/display.mp4");
		gate.resolve();
		expect((await results).map((result) => result.status)).toEqual([
			"rejected",
			"fulfilled",
		]);
	});
});
