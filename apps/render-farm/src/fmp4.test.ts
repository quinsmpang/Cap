import { describe, expect, test } from "bun:test";
import {
	ANNEX_B_PARAMETER_SETS,
	child,
	parseBoxes,
	u32,
	u64,
} from "./boxes.test-util";
import { initSegment, playlist, segmentHeader } from "./fmp4";
import { avcC } from "./mp4";

describe("initSegment", () => {
	test("declares both tracks as fragmented", () => {
		const init = initSegment({
			width: 1664,
			height: 1080,
			fps: 30,
			avcC: avcC(ANNEX_B_PARAMETER_SETS),
			asc: Uint8Array.of(0x11, 0x90),
		});
		const [ftyp, moov] = parseBoxes(init);
		expect(ftyp?.type).toBe("ftyp");
		expect(moov?.type).toBe("moov");
		if (!moov) throw new Error("no moov");
		const children = parseBoxes(moov.body).map((box) => box.type);
		expect(children).toEqual(["mvhd", "trak", "trak", "mvex"]);
		const mvex = child(moov, "mvex");
		expect(mvex && parseBoxes(mvex.body).map((box) => box.type)).toEqual([
			"trex",
			"trex",
		]);
	});

	test("video only when there is no audio", () => {
		const init = initSegment({
			width: 1920,
			height: 1080,
			fps: 60,
			avcC: avcC(ANNEX_B_PARAMETER_SETS),
			asc: null,
		});
		const moov = parseBoxes(init)[1];
		if (!moov) throw new Error("no moov");
		expect(
			parseBoxes(moov.body).filter((box) => box.type === "trak"),
		).toHaveLength(1);
	});
});

describe("segmentHeader", () => {
	test("large payload byte counts keep the original addition order before wrapping", () => {
		const header = segmentHeader({
			sequence: 1,
			firstFrame: 0,
			videoSizes: [2 ** 56, 0, 0],
			firstPacket: 0,
			audioSizes: [1],
		});
		const moof = parseBoxes(header, 0, header.byteLength - 8).find(
			(box) => box.type === "moof",
		);
		if (!moof) throw new Error("missing moof");
		const tracks = parseBoxes(moof.body).filter((box) => box.type === "traf");
		const audio = tracks[1] && child(tracks[1], "trun");
		expect(audio && u32(audio.body, 8)).toBe(208);
	});

	test("empty sample tables retain their data offsets", () => {
		const header = segmentHeader({
			sequence: 1,
			firstFrame: 0,
			videoSizes: [],
			firstPacket: 0,
			audioSizes: [],
		});
		const moof = parseBoxes(header).find((box) => box.type === "moof");
		const traf = moof && child(moof, "traf");
		const trun = traf && child(traf, "trun");
		expect(trun && u32(trun.body, 4)).toBe(0);
		expect(trun && u32(trun.body, 8)).toBe((moof?.size ?? 0) + 8);
	});

	test("data offsets preserve uint32 wrapping and decode timestamps remain 64-bit", () => {
		const input = {
			sequence: 1,
			firstFrame: 0x1_0000_0000,
			videoSizes: [0xffffffff, 101],
			firstPacket: 0x1_0000_0000,
			audioSizes: [23],
		};
		const header = segmentHeader(input);
		const moof = parseBoxes(header).find((box) => box.type === "moof");
		if (!moof) throw new Error("missing moof");
		const tracks = parseBoxes(moof.body).filter((box) => box.type === "traf");
		const video = tracks[0] && child(tracks[0], "trun");
		const audio = tracks[1] && child(tracks[1], "trun");
		const videoTime = tracks[0] && child(tracks[0], "tfdt");
		const audioTime = tracks[1] && child(tracks[1], "tfdt");
		expect(video && u32(video.body, 8)).toBe(moof.size + 8);
		expect(audio && u32(audio.body, 8)).toBe(
			(moof.size + 8 + 0x1_0000_0000 + 100) >>> 0,
		);
		expect(videoTime && u64(videoTime.body, 4)).toBe(input.firstFrame * 1000);
		expect(audioTime && u64(audioTime.body, 4)).toBe(input.firstPacket * 1024);
		expect(input.videoSizes).toEqual([0xffffffff, 101]);
	});

	test("trun offsets point at the video then audio bytes after the header", () => {
		const videoSizes = [100, 50, 50];
		const audioSizes = [20, 30];
		const header = segmentHeader({
			sequence: 121,
			firstFrame: 120,
			videoSizes,
			firstPacket: 10,
			audioSizes,
		});
		const [styp, moof, mdat] = parseBoxes(header);
		expect([styp?.type, moof?.type, mdat?.type]).toEqual([
			"styp",
			"moof",
			"mdat",
		]);
		if (!moof || !mdat) throw new Error("missing boxes");
		expect(u32(header, mdat.start)).toBe(8 + 200 + 50);
		expect(mdat.start + 8).toBe(header.byteLength);

		const mfhd = child(moof, "mfhd");
		expect(mfhd && u32(mfhd.body, 4)).toBe(121);
		const [videoTraf, audioTraf] = parseBoxes(moof.body).filter(
			(box) => box.type === "traf",
		);
		if (!videoTraf || !audioTraf) throw new Error("missing trafs");

		const videoTfdt = child(videoTraf, "tfdt");
		expect(videoTfdt && u64(videoTfdt.body, 4)).toBe(120 * 1000);
		const videoTrun = child(videoTraf, "trun");
		if (!videoTrun) throw new Error("no video trun");
		expect(u32(videoTrun.body, 4)).toBe(3);
		expect(u32(videoTrun.body, 8)).toBe(moof.size + 8);
		expect(u32(videoTrun.body, 12 + 8)).toBe(0x02000000);
		expect(u32(videoTrun.body, 24 + 8)).toBe(0x01010000);

		const audioTfdt = child(audioTraf, "tfdt");
		expect(audioTfdt && u64(audioTfdt.body, 4)).toBe(10 * 1024);
		const audioTrun = child(audioTraf, "trun");
		if (!audioTrun) throw new Error("no audio trun");
		expect(u32(audioTrun.body, 4)).toBe(2);
		expect(u32(audioTrun.body, 8)).toBe(moof.size + 8 + 200);
	});

	test("video-only segments carry one traf", () => {
		const header = segmentHeader({
			sequence: 1,
			firstFrame: 0,
			videoSizes: [10],
			firstPacket: 0,
			audioSizes: [],
		});
		const moof = parseBoxes(header)[1];
		if (!moof) throw new Error("no moof");
		expect(
			parseBoxes(moof.body).filter((box) => box.type === "traf"),
		).toHaveLength(1);
	});
});

describe("playlist", () => {
	test("lists segments in order and ends only when told to", () => {
		const segments = [
			{ url: "a.m4s", duration: 2 },
			{ url: "b.m4s", duration: 1.5 },
		];
		const open = playlist(segments, {
			initUrl: "init.mp4",
			targetDuration: 4,
			ended: false,
		});
		expect(open).toContain('#EXT-X-MAP:URI="init.mp4"');
		expect(open).toContain("#EXT-X-PLAYLIST-TYPE:EVENT");
		expect(open.indexOf("a.m4s")).toBeLessThan(open.indexOf("b.m4s"));
		expect(open).toContain("#EXTINF:1.500,");
		expect(open).not.toContain("#EXT-X-ENDLIST");
		expect(
			playlist(segments, {
				initUrl: "init.mp4",
				targetDuration: 4,
				ended: true,
			}),
		).toContain("#EXT-X-ENDLIST");
	});
});
