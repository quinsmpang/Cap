import { describe, expect, test } from "bun:test";
import {
	ANNEX_B_PARAMETER_SETS,
	child,
	parseBoxes,
	u32,
	u64,
} from "./boxes.test-util";
import {
	avcC,
	build,
	buildHeader,
	byteRangeFor,
	indexVideoTrack,
	type TrackIndex,
} from "./mp4";

function syntheticIndex(samples: number, gop: number): TrackIndex {
	const times = new Float64Array(samples);
	const offsets = new Float64Array(samples);
	const sizes = new Uint32Array(samples);
	const keyframes: number[] = [];
	let offset = 48;
	for (let sample = 0; sample < samples; sample++) {
		times[sample] = sample / 30 + (sample % 7) * 0.0001;
		sizes[sample] = 1000 + ((sample * 7919) % 5000);
		offsets[sample] = offset;
		offset += sizes[sample] ?? 0;
		if (sample % gop === 0) keyframes.push(sample);
	}
	return {
		timescale: 30_000,
		times,
		offsets,
		sizes,
		keyframes: Uint32Array.from(keyframes),
	};
}

// The linear scan byteRangeFor used before the binary search.
function referenceRange(index: TrackIndex, from: number, to: number) {
	const count = index.times.length;
	let first = 0;
	for (const key of index.keyframes) {
		if ((index.times[key] ?? 0) <= from) first = key;
		else break;
	}
	let last = count - 1;
	for (let sample = first; sample < count; sample++) {
		if ((index.times[sample] ?? 0) > to) {
			last = sample;
			break;
		}
	}
	let start = Number.POSITIVE_INFINITY;
	let end = 0;
	for (let sample = first; sample <= last; sample++) {
		const offset = index.offsets[sample] ?? 0;
		start = Math.min(start, offset);
		end = Math.max(end, offset + (index.sizes[sample] ?? 0));
	}
	return { start, end };
}

describe("byteRangeFor", () => {
	test("matches a linear scan across the whole file", () => {
		const index = syntheticIndex(6000, 21);
		for (let from = -1; from < 205; from += 0.37) {
			for (const span of [0, 0.5, 2, 9]) {
				expect(byteRangeFor(index, from, from + span)).toEqual(
					referenceRange(index, from, from + span),
				);
			}
		}
	});

	test("starts at the keyframe at or before the requested time", () => {
		const index = syntheticIndex(300, 30);
		const range = byteRangeFor(index, 2.5, 2.6);
		expect(range?.start).toBe(index.offsets[60]);
	});

	test("includes the next sample at an exact endpoint with non-monotonic offsets", () => {
		const index = syntheticIndex(6, 6);
		index.offsets = Float64Array.of(5000, 100, 4000, 50, 3000, 25);
		index.sizes = new Uint32Array(6).fill(10);
		expect(byteRangeFor(index, 0, index.times[2] ?? 0)).toEqual({
			start: 50,
			end: 5010,
		});
	});

	test("a reversed range still includes the selected keyframe", () => {
		const index = syntheticIndex(300, 30);
		const sample = 60;
		expect(byteRangeFor(index, 2.5, 1)).toEqual({
			start: index.offsets[sample] ?? 0,
			end: (index.offsets[sample] ?? 0) + (index.sizes[sample] ?? 0),
		});
	});

	test("an empty track has no range", () => {
		expect(
			byteRangeFor(
				{
					timescale: 1,
					times: new Float64Array(),
					offsets: new Float64Array(),
					sizes: new Uint32Array(),
					keyframes: new Uint32Array(),
				},
				0,
				1,
			),
		).toBeNull();
	});
});

describe("binary writer", () => {
	test("scalar encoding remains big-endian with the existing overflow rules", () => {
		const bytes = build((writer) => {
			writer.u8(0x123);
			writer.u16(0x12345);
			writer.u32(0x1_0000_0001);
			writer.u64(0x1_0000_0000);
			writer.u64(-1);
			writer.i16(-2);
			writer.u16(-1);
			writer.u32(Number.NaN);
		});
		expect(bytes).toEqual(
			Uint8Array.from([
				0x23,
				0x23,
				0x45,
				0,
				0,
				0,
				1,
				0,
				0,
				0,
				1,
				0,
				0,
				0,
				0,
				...new Array<number>(8).fill(0xff),
				0xff,
				0xfe,
				0xff,
				0xff,
				0,
				0,
				0,
				0,
			]),
		);
	});

	test("scalar writes stay contiguous across page boundaries", () => {
		for (const offset of [
			0, 1, 255, 256, 767, 768, 1791, 1792, 3839, 3840, 7935, 7936,
		]) {
			for (const [method, value, suffix] of [
				["u16", 0x1234, [0x12, 0x34]],
				["u32", 0x12345678, [0x12, 0x34, 0x56, 0x78]],
				["u64", 0x1_2345_6789, [0, 0, 0, 1, 0x23, 0x45, 0x67, 0x89]],
			] as const) {
				const expected = new Uint8Array(offset + suffix.length).fill(
					0xab,
					0,
					offset,
				);
				expected.set(suffix, offset);
				expect(
					build((writer) => {
						for (let i = 0; i < offset; i++) writer.u8(0xab);
						writer[method](value);
					}),
				).toEqual(expected);
			}
		}
	});

	test("byte views keep their lazy reference semantics while finished snapshots stay independent", () => {
		const source = Uint8Array.of(10, 20, 30, 40);
		const bytes = build((writer) => {
			writer.u16(0x1122);
			writer.bytes(source.subarray(1, 3));
			writer.bytes(new Uint8Array());
			writer.u8(0x33);
			const first = writer.finish();
			source[1] = 0x44;
			const second = writer.finish();
			expect(first).toEqual(Uint8Array.of(0x11, 0x22, 20, 30, 0x33));
			expect(second).toEqual(Uint8Array.of(0x11, 0x22, 0x44, 30, 0x33));
			expect(second.buffer).not.toBe(first.buffer);
			writer.u8(0x55);
		});
		expect(bytes).toEqual(Uint8Array.of(0x11, 0x22, 0x44, 30, 0x33, 0x55));
	});

	test("failed u64 conversions cannot advance length or leave holes", () => {
		for (const prefix of [1, 255, 256, 7935]) {
			const bytes = build((writer) => {
				for (let i = 0; i < prefix; i++) writer.u8(7);
				for (const value of [Number.NaN, 1.5, Number.POSITIVE_INFINITY]) {
					expect(() => writer.u64(value)).toThrow();
					expect(writer.length).toBe(prefix);
				}
				writer.u16(0x1234);
			});
			const expected = new Uint8Array(prefix + 2).fill(7, 0, prefix);
			expected.set([0x12, 0x34], prefix);
			expect(bytes).toEqual(expected);
		}
	});

	test("text and zero padding keep their encoding and count conversion", () => {
		const bytes = build((writer) => {
			writer.ascii("café");
			writer.zeros(3.5);
			expect(() => writer.zeros(-1)).toThrow();
			writer.u8(9);
		});
		expect(bytes).toEqual(
			Uint8Array.of(0x63, 0x61, 0x66, 0xc3, 0xa9, 0, 0, 0, 9),
		);
	});
});

describe("buildHeader", () => {
	test("rebases every track without changing Number rounding or the input tables", () => {
		const input: Parameters<typeof buildHeader>[0] = {
			width: 1920,
			height: 1080,
			fps: 60,
			video: {
				sizes: Uint32Array.of(10, 20, 30, 40),
				runs: [
					{ first: 0, count: 2, offset: 100 },
					{ first: 2, count: 2, offset: Number.MAX_SAFE_INTEGER },
				],
				keyframes: Uint32Array.of(0, 2),
				avcC: avcC(ANNEX_B_PARAMETER_SETS),
			},
			audio: {
				sizes: Uint32Array.of(5, 6),
				runs: [
					{ first: 0, count: 1, offset: -4 },
					{ first: 1, count: 1, offset: 200 },
				],
				asc: Uint8Array.of(0x11, 0x90),
				totalSamples: 1024,
				priming: 1024,
			},
			payloadSize: 0x1_0000_0000 + 123,
			minimumSize: 4096,
		};
		const tables = [input.video, input.audio];
		const original = structuredClone(tables);
		for (const table of tables) {
			if (!table) throw new Error("missing fixture track");
			for (const run of table.runs) Object.freeze(run);
			Object.freeze(table.runs);
		}
		const header = buildHeader(input);
		const boxes = parseBoxes(header);
		const moov = boxes.find((box) => box.type === "moov");
		if (!moov) throw new Error("missing moov");
		const tracks = parseBoxes(moov.body).filter((box) => box.type === "trak");
		for (const [index, track] of tracks.entries()) {
			const mdia = child(track, "mdia");
			const minf = mdia && child(mdia, "minf");
			const stbl = minf && child(minf, "stbl");
			const offsets = stbl && child(stbl, "co64");
			const source = tables[index];
			if (!offsets || !source) throw new Error("missing chunk offsets");
			const view = new DataView(
				offsets.body.buffer,
				offsets.body.byteOffset,
				offsets.body.byteLength,
			);
			for (const [entry, run] of source.runs.entries()) {
				expect(view.getBigUint64(8 + entry * 8)).toBe(
					BigInt(run.offset + header.byteLength),
				);
			}
		}
		const mdat = boxes.find((box) => box.type === "mdat");
		if (!mdat) throw new Error("missing mdat");
		expect(new DataView(header.buffer).getBigUint64(mdat.start + 8)).toBe(
			BigInt(16 + input.payloadSize),
		);
		expect(tables).toEqual(original);
		expect(buildHeader(input)).toEqual(header);
	});

	test("padding one to seven bytes above bare size still creates an eight-byte free box", () => {
		const input: Parameters<typeof buildHeader>[0] = {
			width: 128,
			height: 72,
			fps: 30,
			video: {
				sizes: Uint32Array.of(10),
				runs: [{ first: 0, count: 1, offset: 0 }],
				keyframes: Uint32Array.of(0),
				avcC: avcC(ANNEX_B_PARAMETER_SETS),
			},
			audio: null,
			payloadSize: 10,
			minimumSize: 0,
		};
		const bare = buildHeader(input).byteLength;
		for (let extra = 1; extra <= 7; extra++) {
			const padded = buildHeader({ ...input, minimumSize: bare + extra });
			expect(padded.byteLength).toBe(bare + 8);
			expect(parseBoxes(padded).find((box) => box.type === "free")?.size).toBe(
				8,
			);
		}
	});

	test("empty chunk tables still produce a valid empty offset table", () => {
		const header = buildHeader({
			width: 128,
			height: 72,
			fps: 30,
			video: {
				sizes: new Uint32Array(),
				runs: [],
				keyframes: new Uint32Array(),
				avcC: avcC(ANNEX_B_PARAMETER_SETS),
			},
			audio: null,
			payloadSize: 0,
			minimumSize: 0,
		});
		const moov = parseBoxes(header).find((box) => box.type === "moov");
		const track = moov && child(moov, "trak");
		const mdia = track && child(track, "mdia");
		const minf = mdia && child(mdia, "minf");
		const stbl = minf && child(minf, "stbl");
		const offsets = stbl && child(stbl, "co64");
		expect(offsets && u32(offsets.body, 4)).toBe(0);
	});

	test("invalid offsets still fail before a header can be returned", () => {
		for (const offset of [1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
			expect(() =>
				buildHeader({
					width: 128,
					height: 72,
					fps: 30,
					video: {
						sizes: Uint32Array.of(10),
						runs: [{ first: 0, count: 1, offset }],
						keyframes: Uint32Array.of(0),
						avcC: avcC(ANNEX_B_PARAMETER_SETS),
					},
					audio: null,
					payloadSize: 10,
					minimumSize: 0,
				}),
			).toThrow();
		}
	});

	test("lays out ftyp, moov, padding and a 64-bit mdat header", () => {
		const header = buildHeader({
			width: 1920,
			height: 1080,
			fps: 30,
			video: {
				sizes: Uint32Array.from([100, 200, 300]),
				runs: [{ first: 0, count: 3, offset: 0 }],
				keyframes: Uint32Array.from([0]),
				avcC: avcC(ANNEX_B_PARAMETER_SETS),
			},
			audio: {
				sizes: Uint32Array.from([10, 10]),
				runs: [{ first: 0, count: 2, offset: 600 }],
				asc: Uint8Array.of(0x11, 0x90),
				totalSamples: 1024,
				priming: 1024,
			},
			payloadSize: 620,
			minimumSize: 64 * 1024,
		});
		expect(header.byteLength).toBe(64 * 1024);
		const boxes = parseBoxes(header);
		expect(boxes.map((box) => box.type)).toEqual([
			"ftyp",
			"moov",
			"free",
			"mdat",
		]);
		const mdat = boxes[3];
		expect(u32(header, mdat?.start ?? 0)).toBe(1);
		expect(u64(header, (mdat?.start ?? 0) + 8)).toBe(16 + 620);
		const moov = boxes[1];
		if (!moov) throw new Error("no moov");
		const traks = parseBoxes(moov.body).filter((box) => box.type === "trak");
		expect(traks).toHaveLength(2);
		const stbl = child(
			child(child(traks[0] ?? moov, "mdia") ?? moov, "minf") ?? moov,
			"stbl",
		);
		const co64 = stbl && child(stbl, "co64");
		expect(co64 && u64(co64.body, 8)).toBe(header.byteLength);
	});
});

describe("indexVideoTrack", () => {
	const u32s = (...values: number[]) => {
		const bytes = new Uint8Array(values.length * 4);
		const view = new DataView(bytes.buffer);
		for (const [index, value] of values.entries()) {
			view.setUint32(index * 4, value);
		}
		return bytes;
	};
	const box = (type: string, ...payloads: Uint8Array[]) => {
		const size = 8 + payloads.reduce((sum, part) => sum + part.byteLength, 0);
		const bytes = new Uint8Array(size);
		new DataView(bytes.buffer).setUint32(0, size);
		bytes.set(new TextEncoder().encode(type), 4);
		let offset = 8;
		for (const part of payloads) {
			bytes.set(part, offset);
			offset += part.byteLength;
		}
		return bytes;
	};
	const track = (tables: Partial<Record<string, Uint8Array>>) =>
		box(
			"moov",
			box(
				"trak",
				box(
					"mdia",
					box("mdhd", u32s(0, 0, 0, 30, 0, 0)),
					box("hdlr", u32s(0, 0), new TextEncoder().encode("vide")),
					box(
						"minf",
						box(
							"stbl",
							...Object.entries({
								stsz: box("stsz", u32s(0, 0, 3, 10, 20, 30)),
								stts: box("stts", u32s(0, 1, 3, 1)),
								stco: box("stco", u32s(0, 1, 1000)),
								stsc: box("stsc", u32s(0, 1, 1, 3, 1)),
								stss: box("stss", u32s(0, 1, 1)),
								...tables,
							}).map(([, bytes]) => bytes as Uint8Array),
						),
					),
				),
			),
		);

	test("indexes sizes, offsets, times and keyframes", () => {
		const index = indexVideoTrack(track({}));
		expect([...index.sizes]).toEqual([10, 20, 30]);
		expect([...index.offsets]).toEqual([1000, 1010, 1030]);
		expect([...index.times]).toEqual([0, 1 / 30, 2 / 30]);
		expect([...index.keyframes]).toEqual([0]);
	});

	test.each([
		[
			"a uniform sample size with a huge count",
			"video samples",
			{ stsz: box("stsz", u32s(0, 10, 0xffffffff)) },
		],
		[
			"a sample table shorter than its count",
			"stsz lists more entries",
			{ stsz: box("stsz", u32s(0, 0, 1000, 10)) },
		],
		[
			"a chunk offset table shorter than its count",
			"stco lists more entries",
			{ stco: box("stco", u32s(0, 0x7fffffff, 1000)) },
		],
		[
			"a sync sample table shorter than its count",
			"stss lists more entries",
			{ stss: box("stss", u32s(0, 50, 1)) },
		],
	])("rejects %s", (_, message, tables) => {
		expect(() => indexVideoTrack(track(tables))).toThrow(message);
	});

	test("a sample-to-chunk run past the offset table ends at the last chunk", () => {
		const index = indexVideoTrack(
			track({ stsc: box("stsc", u32s(0, 2, 1, 1, 1, 0xfffffff0, 1, 1)) }),
		);
		expect(index.offsets[0]).toBe(1000);
	});
});
