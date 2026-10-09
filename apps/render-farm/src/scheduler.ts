export type SchedulableTask =
	| { kind: "video"; jobId: string; chunk: number }
	| { kind: "audio"; jobId: string; section: number };

export type SchedulableState = {
	task: SchedulableTask;
	state: "queued" | "running" | "done";
	heldUntil?: number;
};

export type SchedulableJob = {
	id: string;
	status: string;
	requestedAt: number;
	/** Chunk indices in playback order. */
	chunks: number[];
	finishedChunks: { has(chunk: number): boolean };
	runningTasks: number;
};

export type SchedulerOptions = {
	/** Unfinished chunks per job, from the front, that outrank other work. */
	headChunks: number;
	fifo: boolean;
	now: number;
};

/**
 * Index into `queue` of the task a free slot should take, or -1.
 *
 * FIFO lets one long export hold every slot while a short one (and its first
 * playable segment) waits minutes. Instead the order is:
 *  1. playback-gating work: each job's next unfinished chunks in playback
 *     order (an HLS playlist only grows from the front) and its first audio
 *     section;
 *  2. fairness: the job currently holding the fewest running tasks;
 *  3. the older job, then the earlier position in the timeline.
 */
export function pickQueued(
	queue: readonly SchedulableState[],
	jobs: Iterable<SchedulableJob>,
	accepts: (kind: SchedulableTask["kind"]) => boolean,
	options: SchedulerOptions,
) {
	const byId = new Map<string, { job: SchedulableJob; head: Set<number> }>();
	const eligible = (state: SchedulableState) =>
		state.state === "queued" &&
		byId.has(state.task.jobId) &&
		accepts(state.task.kind) &&
		!(state.heldUntil && state.heldUntil > options.now);
	for (const job of jobs) {
		if (job.status !== "rendering") continue;
		const head = new Set<number>();
		for (const chunk of job.chunks) {
			if (job.finishedChunks.has(chunk)) continue;
			head.add(chunk);
			if (head.size >= options.headChunks) break;
		}
		byId.set(job.id, { job, head });
	}
	if (options.fifo) return queue.findIndex(eligible);

	let best = -1;
	let bestPriority = 0;
	let bestRunningTasks = 0;
	let bestRequestedAt = 0;
	let bestPosition = 0;
	for (const [index, state] of queue.entries()) {
		if (!eligible(state)) continue;
		const task = state.task;
		const entry = byId.get(task.jobId);
		if (!entry) continue;
		const position = task.kind === "video" ? task.chunk : task.section;
		const gating =
			task.kind === "video" ? entry.head.has(task.chunk) : position === 0;
		const priority = gating ? 0 : 1;
		const runningTasks = entry.job.runningTasks;
		const requestedAt = entry.job.requestedAt;
		let comparison = priority - bestPriority;
		if (comparison === 0) comparison = runningTasks - bestRunningTasks;
		if (comparison === 0) comparison = requestedAt - bestRequestedAt;
		if (comparison === 0) comparison = position - bestPosition;
		if (best < 0 || comparison < 0) {
			best = index;
			bestPriority = priority;
			bestRunningTasks = runningTasks;
			bestRequestedAt = requestedAt;
			bestPosition = position;
		}
	}
	return best;
}
