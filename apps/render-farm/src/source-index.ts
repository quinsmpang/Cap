import { ConcurrencyLimiter } from "./concurrency-limiter";
import { indexVideoTrack, locateMoov, type TrackIndex } from "./mp4";
import type { S3 } from "./s3";
import { checkManifestBounds, isKey, sourceLimitsFromEnv } from "./validate";

export type Manifest = {
	files: {
		path: string;
		size: number;
		key?: string;
		transcodeFrom?: string;
	}[];
};

export type RecordingMeta = {
	segments?: {
		display: { path: string; start_time?: number };
		camera?: { path: string; start_time?: number };
		mic?: { path: string };
		system_audio?: { path: string };
	}[];
	display?: { path: string };
	camera?: { path: string };
	audio?: { path: string };
};

type Mp4Meta = {
	head: [number, number];
	moov: [number, number];
	index: TrackIndex;
};

export type SourceIndex = {
	manifest: Manifest;
	recordingMeta: RecordingMeta;
	mediaMeta: Map<string, Mp4Meta & { size: number; key: string }>;
};

type PendingIndex = {
	manifest: Promise<Manifest>;
	index: Promise<SourceIndex>;
};

export class SourceIndexes {
	private readonly indexes = new Map<string, SourceIndex>();
	private readonly pending = new Map<string, PendingIndex>();
	private readonly mp4Metas = new Map<string, Mp4Meta>();
	private readonly pendingMp4Metas = new Map<string, Promise<Mp4Meta>>();
	private readonly limits: ReturnType<typeof sourceLimitsFromEnv>;
	private readonly prefixes: string[];
	private readonly readLimiter: ConcurrencyLimiter;

	constructor(
		private readonly storage: Pick<S3, "getRange" | "getRangeTagged">,
		private readonly transcodes: {
			storedSize: (output: string) => Promise<number | null>;
			sourceSize: (source: string) => Promise<number>;
			run: (source: string, output: string) => Promise<number>;
		},
		private readonly env: Record<string, string | undefined> = process.env,
	) {
		this.limits = sourceLimitsFromEnv(env);
		this.prefixes = (env.RF_SOURCE_KEY_PREFIXES ?? "")
			.split(",")
			.filter(Boolean);
		const concurrency = Math.max(
			1,
			Math.floor(Number(env.RF_INDEX_READ_CONCURRENCY)) || 16,
		);
		this.readLimiter = new ConcurrencyLimiter(
			Number.isFinite(concurrency) ? concurrency : 16,
		);
	}

	async get(
		prefix: string,
		sourceRoot?: string,
		onManifest?: (manifest: Manifest) => void,
	): Promise<SourceIndex> {
		const cacheEnabled = this.env.RF_INDEX_CACHE !== "0";
		const cached = cacheEnabled ? this.indexes.get(prefix) : undefined;
		if (cached) {
			this.checkManifest(cached.manifest, prefix, sourceRoot);
			onManifest?.(cached.manifest);
			return cached;
		}
		const key = JSON.stringify([prefix, sourceRoot ?? null]);
		let pending = cacheEnabled ? this.pending.get(key) : undefined;
		if (!pending) {
			const manifest = this.readManifest(prefix, sourceRoot);
			const index = manifest
				.then(async (manifest) => {
					// Let each export start its own downloads before shared indexing begins.
					await Promise.resolve();
					return this.build(prefix, manifest);
				})
				.then((index) => {
					this.indexes.set(prefix, index);
					if (this.indexes.size > 32) {
						const oldest = this.indexes.keys().next().value;
						if (oldest !== undefined) this.indexes.delete(oldest);
					}
					return index;
				})
				.finally(() => {
					if (cacheEnabled) this.pending.delete(key);
				});
			pending = { manifest, index };
			if (cacheEnabled) this.pending.set(key, pending);
		}
		const [index] = await Promise.all([
			pending.index,
			pending.manifest.then((manifest) => {
				onManifest?.(manifest);
			}),
		]);
		return index;
	}

	private async readManifest(prefix: string, sourceRoot?: string) {
		const manifest = JSON.parse(
			new TextDecoder().decode(
				await this.getBounded(
					`${prefix}/manifest.json`,
					this.limits.metadataBytes,
				),
			),
		) as Manifest;
		this.checkManifest(manifest, prefix, sourceRoot);
		return manifest;
	}

	private checkManifest(
		manifest: Manifest,
		prefix: string,
		sourceRoot?: string,
	) {
		const bounds = checkManifestBounds(manifest, this.limits);
		if (bounds) throw new Error(bounds);
		for (const file of manifest.files) {
			const parts = file.path.split("/");
			if (
				file.path.startsWith("/") ||
				parts.includes("..") ||
				parts.includes("")
			) {
				throw new Error(`manifest path ${file.path} is not a relative path`);
			}
			const inScope = (key: string) =>
				key.startsWith(`${prefix}/`) ||
				(sourceRoot !== undefined && key.startsWith(sourceRoot)) ||
				this.prefixes.some((allowed) => key.startsWith(allowed));
			for (const key of [file.key, file.transcodeFrom]) {
				if (key !== undefined && (!isKey(key) || !inScope(key))) {
					throw new Error(
						`manifest key for ${file.path} is outside the recording`,
					);
				}
			}
			if (file.transcodeFrom !== undefined && file.key === undefined) {
				throw new Error(`manifest transcode for ${file.path} names no key`);
			}
		}
	}

	private async build(
		prefix: string,
		manifest: Manifest,
	): Promise<SourceIndex> {
		const keyOf = (file: { path: string; key?: string }) =>
			file.key ?? `${prefix}/${file.path}`;
		const sourceFiles = await Promise.all(
			manifest.files.map(async (file) => ({
				...file,
				size:
					file.transcodeFrom === undefined
						? file.size
						: ((await this.transcodes.storedSize(keyOf(file))) ??
							(await this.transcodes.sourceSize(file.transcodeFrom))),
			})),
		);
		const sourceBounds = checkManifestBounds(
			{ files: sourceFiles },
			this.limits,
		);
		if (sourceBounds) throw new Error(sourceBounds);
		await Promise.all(
			manifest.files.map(async (file) => {
				if (file.transcodeFrom === undefined) return;
				file.size = await this.transcodes.run(file.transcodeFrom, keyOf(file));
			}),
		);
		const bounds = checkManifestBounds(manifest, this.limits);
		if (bounds) throw new Error(bounds);
		const metaFile = manifest.files.find(
			(file) => file.path === "recording-meta.json",
		);
		if (!metaFile) throw new Error("recording has no recording-meta.json");
		const mediaMeta: SourceIndex["mediaMeta"] = new Map();
		const [recordingMeta] = await Promise.all([
			this.getBounded(keyOf(metaFile), this.limits.metadataBytes).then(
				(bytes) => JSON.parse(new TextDecoder().decode(bytes)) as RecordingMeta,
			),
			...manifest.files
				.filter((file) => file.path.endsWith(".mp4"))
				.map(async (file) => {
					const meta = await this.mp4MetaRanges(keyOf(file), file.size);
					mediaMeta.set(file.path, {
						...meta,
						size: file.size,
						key: keyOf(file),
					});
				}),
		]);
		return { manifest, recordingMeta, mediaMeta };
	}

	private async getBounded(key: string, limit: number) {
		const bytes = await this.storage.getRange(key, 0, limit);
		if (bytes.byteLength > limit) {
			throw new Error(`${key} is larger than ${limit} bytes`);
		}
		return bytes;
	}

	private async mp4MetaRanges(key: string, size: number): Promise<Mp4Meta> {
		const result = await this.readLimiter.run(async () => {
			const headEnd = Math.min(size, 128 * 1024);
			const { bytes: head, etag } = await this.storage.getRangeTagged(
				key,
				0,
				headEnd - 1,
			);
			const cacheKey =
				etag && this.env.RF_INDEX_CACHE !== "0"
					? `${key}\n${size}\n${etag}`
					: null;
			const cached = cacheKey ? this.mp4Metas.get(cacheKey) : undefined;
			if (cacheKey && cached) {
				this.mp4Metas.delete(cacheKey);
				this.mp4Metas.set(cacheKey, cached);
				return cached;
			}
			const pending = cacheKey ? this.pendingMp4Metas.get(cacheKey) : undefined;
			if (pending) {
				// Keeping follower slots occupied would stall unrelated sources behind one shared read.
				return { pending };
			}
			const reading = this.readMp4Meta(key, size, head);
			if (!cacheKey) return reading;
			const indexed = reading
				.then((meta) => {
					this.mp4Metas.set(cacheKey, meta);
					if (this.mp4Metas.size > 16) {
						const oldest = this.mp4Metas.keys().next().value;
						if (oldest !== undefined) this.mp4Metas.delete(oldest);
					}
					return meta;
				})
				.finally(() => this.pendingMp4Metas.delete(cacheKey));
			this.pendingMp4Metas.set(cacheKey, indexed);
			return indexed;
		});
		return "pending" in result ? result.pending : result;
	}

	private async readMp4Meta(
		key: string,
		size: number,
		head: Uint8Array,
	): Promise<Mp4Meta> {
		const location = locateMoov(head, size);
		let moovStart: number;
		let moovBytes: Uint8Array;
		if (location && "start" in location && location.start !== undefined) {
			if (location.size > this.limits.moovBytes) {
				throw new Error(`${key} has a ${location.size} byte moov`);
			}
			moovStart = location.start;
			moovBytes =
				location.start + location.size <= head.byteLength
					? head.subarray(location.start, location.start + location.size)
					: await this.storage.getRange(
							key,
							location.start,
							location.start + location.size - 1,
						);
		} else if (location && "next" in location && location.next !== undefined) {
			if (size - location.next > this.limits.moovBytes) {
				throw new Error(`${key} has ${size - location.next} bytes after mdat`);
			}
			const tail = await this.storage.getRange(key, location.next, size - 1);
			const found = locateMoov(tail, tail.byteLength);
			if (!found || !("start" in found) || found.start === undefined) {
				throw new Error(`no moov in ${key}`);
			}
			moovStart = location.next + found.start;
			moovBytes = tail.subarray(found.start, found.start + found.size);
		} else {
			throw new Error(`no moov in ${key}`);
		}
		return {
			head: [0, Math.min(size, 128 * 1024)],
			moov: [moovStart, moovStart + moovBytes.byteLength],
			index: indexVideoTrack(moovBytes),
		};
	}
}
