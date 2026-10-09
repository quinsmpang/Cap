import { ConcurrencyLimiter } from "./concurrency-limiter";
import type { S3 } from "./s3";

export class TranscodeInputs {
	private readonly pending = new Map<string, ReturnType<S3["head"]>>();
	private readonly limiter: ConcurrencyLimiter;

	constructor(
		private readonly storage: Pick<S3, "head">,
		private readonly sourceBytes: number,
		concurrency = 16,
	) {
		this.limiter = new ConcurrencyLimiter(concurrency);
	}

	async storedSize(output: string) {
		const head = await this.head(output);
		if (!head) return null;
		if (
			!Number.isSafeInteger(head.size) ||
			head.size <= 0 ||
			head.size > this.sourceBytes
		) {
			throw new Error("stored transcode output has an invalid size");
		}
		return head.size;
	}

	async sourceSize(source: string) {
		const head = await this.head(source);
		if (!head || !Number.isSafeInteger(head.size) || head.size <= 0) {
			throw new Error(
				`transcode source ${source} is missing or has an invalid size`,
			);
		}
		if (head.size > this.sourceBytes) {
			throw new Error(
				`transcode source ${source} is ${head.size} bytes (limit ${this.sourceBytes})`,
			);
		}
		return head.size;
	}

	private head(key: string) {
		const existing = this.pending.get(key);
		if (existing) return existing;
		const reading = this.limiter
			.run(() => this.storage.head(key))
			.finally(() => {
				this.pending.delete(key);
			});
		this.pending.set(key, reading);
		return reading;
	}
}
