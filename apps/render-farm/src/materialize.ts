import {
	closeSync,
	ftruncateSync,
	mkdirSync,
	openSync,
	readFileSync,
	writeFileSync,
	writeSync,
} from "node:fs";
import { dirname, isAbsolute, join, parse, resolve, sep } from "node:path";
import { ConcurrencyLimiter } from "./concurrency-limiter";
import type { S3 } from "./s3";

// Recreates a recording on local disk with only the bytes a task needs.
// Media files are created at full size as sparse files, and just the
// requested byte ranges (the moov, plus the samples around the task's time
// span) are written in. ffmpeg then opens them like the real recording, with
// correct timestamps and seeking, so the renderer needs no changes at all.

export type FileSpec = {
	/** Path inside the project directory. */
	path: string;
	key: string;
	size: number;
	/** Byte ranges [start, end) to fetch, or "all". */
	ranges: [number, number][] | "all";
};

const PIECE = 4 << 20;

type LocalFile = {
	fd: number;
	pieces: Map<number, Promise<void> | null>;
};

export type FetchStats = { bytes: number; requests: number; ms: number };

export class ProjectCache {
	private files = new Map<string, LocalFile>();
	private filesByPath = new Map<string, LocalFile>();
	private readonly cacheFilePaths: boolean;
	private closed = false;
	private rewritten = false;
	private readonly limiter: ConcurrencyLimiter;
	bytesFetched = 0;

	constructor(
		readonly s3: Pick<S3, "getRange">,
		readonly root: string,
		concurrency = Number(process.env.RF_FETCH_CONCURRENCY ?? 12),
	) {
		this.cacheFilePaths =
			isAbsolute(root) &&
			(process.platform !== "win32" || parse(root).root.length > 1);
		const limit =
			Number.isFinite(concurrency) && concurrency > 0
				? Math.max(1, Math.floor(concurrency))
				: 12;
		this.limiter = new ConcurrencyLimiter(limit);
	}

	private assertOpen() {
		if (this.closed) throw new Error("Project cache is closed");
	}

	private open(spec: FileSpec) {
		this.assertOpen();
		const cached = this.cacheFilePaths
			? this.filesByPath.get(spec.path)
			: undefined;
		if (cached) return cached;
		const fullPath = resolve(this.root, spec.path);
		if (!fullPath.startsWith(resolve(this.root) + sep)) {
			throw new Error(`${spec.path} is outside the project`);
		}
		let file = this.files.get(fullPath);
		if (!file) {
			mkdirSync(dirname(fullPath), { recursive: true });
			const fd = openSync(fullPath, "w+");
			try {
				ftruncateSync(fd, spec.size);
			} catch (error) {
				closeSync(fd);
				throw error;
			}
			file = { fd, pieces: new Map() };
			this.files.set(fullPath, file);
		}
		if (this.cacheFilePaths) this.filesByPath.set(spec.path, file);
		return file;
	}

	private fetchPiece(
		spec: FileSpec,
		file: LocalFile,
		piece: number,
		stats: FetchStats,
	) {
		let pending = file.pieces.get(piece);
		if (pending === null) return;
		if (!pending) {
			const start = piece * PIECE;
			const end = Math.min(spec.size, start + PIECE);
			pending = this.limiter
				.run(async () => {
					this.assertOpen();
					const bytes = await this.s3.getRange(spec.key, start, end - 1);
					this.assertOpen();
					if (bytes.byteLength !== end - start) {
						throw new Error(
							`short read ${spec.key} ${start}-${end}: ${bytes.byteLength}`,
						);
					}
					let written = 0;
					while (written < bytes.byteLength) {
						written += writeSync(
							file.fd,
							bytes,
							written,
							bytes.byteLength - written,
							start + written,
						);
					}
					this.bytesFetched += bytes.byteLength;
					stats.bytes += bytes.byteLength;
					stats.requests++;
				})
				.then(() => {
					file.pieces.set(piece, null);
				});
			pending.catch(() => file.pieces.delete(piece));
			file.pieces.set(piece, pending);
		}
		return pending;
	}

	async materialize(specs: FileSpec[]): Promise<FetchStats> {
		this.assertOpen();
		const started = performance.now();
		const stats: FetchStats = { bytes: 0, requests: 0, ms: 0 };
		const work: Promise<void>[] = [];
		for (const spec of specs) {
			if (spec.size === 0) {
				this.open(spec);
				continue;
			}
			const file = this.open(spec);
			const ranges: [number, number][] =
				spec.ranges === "all" ? [[0, spec.size]] : spec.ranges;
			const pieces = new Set<number>();
			for (const [start, end] of ranges) {
				const clampedEnd = Math.min(end, spec.size);
				if (clampedEnd <= start) continue;
				for (
					let piece = Math.floor(start / PIECE);
					piece <= Math.floor((clampedEnd - 1) / PIECE);
					piece++
				) {
					pieces.add(piece);
				}
			}
			for (const piece of pieces) {
				const pending = this.fetchPiece(spec, file, piece, stats);
				if (pending) work.push(pending);
			}
		}
		await Promise.all(work);
		this.assertOpen();
		if (
			!this.rewritten &&
			specs.some((spec) => spec.path === "project-config.json")
		) {
			// Configs reference bundled assets (wallpapers) relative to the
			// project; the renderer wants absolute paths.
			const path = join(this.root, "project-config.json");
			const text = readFileSync(path, "utf8");
			writeFileSync(path, text.replaceAll("$RF_PROJECT", this.root));
			this.rewritten = true;
		}
		stats.ms = Math.round(performance.now() - started);
		return stats;
	}

	close() {
		this.closed = true;
		for (const file of this.files.values()) {
			try {
				closeSync(file.fd);
			} catch {}
		}
		this.files.clear();
		this.filesByPath.clear();
	}
}
