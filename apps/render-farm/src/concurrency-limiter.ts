export class ConcurrencyLimiter {
	private active = 0;
	private waiting: ((() => void) | undefined)[] = [];
	private head = 0;

	constructor(private readonly limit: number) {
		if (!Number.isInteger(limit) || limit < 1) {
			throw new RangeError("Concurrency must be a positive integer");
		}
	}

	async run<T>(work: () => Promise<T>): Promise<T> {
		if (this.active < this.limit) this.active++;
		else await new Promise<void>((resolve) => this.waiting.push(resolve));
		try {
			return await work();
		} finally {
			const next = this.waiting[this.head];
			if (next) {
				this.waiting[this.head++] = undefined;
				if (this.head === this.waiting.length) {
					this.waiting = [];
					this.head = 0;
				} else if (this.head >= 1024 && this.head * 2 >= this.waiting.length) {
					this.waiting = this.waiting.slice(this.head);
					this.head = 0;
				}
				next();
			} else {
				this.active--;
			}
		}
	}
}
