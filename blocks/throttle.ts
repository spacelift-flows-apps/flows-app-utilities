import { AppBlock, events, kv, lifecycle, timers } from "@slflows/sdk/v1";

const BUCKET_KEY = "bucket";
const QUEUE_PREFIX = "q:";
const TIMER_KEY = "timerId";

interface BucketState {
  tokens: number;
  lastUpdate: number;
}

interface QueuedEvent {
  pendingEventId: string;
}

const validate = (count: number, interval: number) => {
  if (!(count > 0) || !(interval > 0)) {
    throw new Error(
      `Throttle requires count > 0 and interval > 0 (got count=${count}, interval=${interval})`,
    );
  }
};

const throttle: AppBlock = {
  name: "Throttle",
  category: "Control",
  description:
    "Smooths a bursty event stream into a bounded rate by queueing excess " +
    "events and emitting them as soon as capacity allows. No events are " +
    "discarded.\n\n" +
    "How it works:\n" +
    "- A token bucket holds up to `count` tokens and refills continuously to " +
    "full over `interval` seconds\n" +
    "- Each emitted event consumes one token\n" +
    "- Events that arrive while tokens are available are emitted immediately\n" +
    "- Events that arrive when the bucket is empty wait in an internal queue " +
    "and are emitted in arrival order as tokens refill\n" +
    "- Queued events show up as pending events in the UI so the backlog is " +
    "visible\n\n" +
    "Burst behavior:\n" +
    "- The bucket starts full, so the first `count` events pass through " +
    "instantly even after long idle periods\n" +
    "- Steady-state throughput is bounded by `count / interval` events per second\n" +
    "- Queue drain runs on a timer with 1-second resolution; high `count / " +
    "interval` rates emit in batches at 1-second boundaries rather than evenly " +
    "spaced\n\n" +
    "Use cases:\n" +
    "- API call smoothing: convert a bursty event stream into a steady call rate\n" +
    "- Cost smoothing: avoid spikes hitting paid downstream services\n" +
    "- Backpressure: protect slow consumers from being overwhelmed\n\n" +
    "Companion: use the Rate Limit block instead if you want to divert excess " +
    "events to a separate output rather than queue and delay them.",

  autoconfirm: true,
  config: {
    count: {
      name: "Count",
      description:
        "Maximum number of events emitted per `interval` seconds " +
        "(also the burst capacity).\n\n" +
        "Examples:\n" +
        "- `count: 10`, `interval: 1` → up to 10 events/second\n" +
        "- `count: 100`, `interval: 60` → up to 100 events/minute\n" +
        "- `count: 1`, `interval: 5` → at most one event every 5 seconds",
      type: "number",
      required: true,
      default: 10,
    },
    interval: {
      name: "Interval (seconds)",
      description:
        "Length of the refill window in seconds. The bucket replenishes from " +
        "empty back to full over this duration at a constant rate.",
      type: "number",
      required: true,
      default: 1,
    },
  },

  inputs: {
    default: {
      onEvent: async () => {
        // Create a pending event so the queued backlog shows up in the UI.
        // The predicted body is `{}` to match what gets emitted on completion.
        const pendingEventId = await events.createPending({
          event: {},
          outputKey: "default",
          statusDescription: "Throttled, waiting for rate window",
        });

        // Enqueue with timestamp-prefixed key so onSync drains in arrival order
        // regardless of how the runtime orders KV list results.
        const ts = Date.now().toString().padStart(13, "0");
        await kv.block.set({
          key: `${QUEUE_PREFIX}${ts}:${pendingEventId}`,
          value: { pendingEventId } satisfies QueuedEvent,
        });
        await lifecycle.sync();
      },
    },
  },

  // The bucket update + queue drain is serialized through onSync. The runtime
  // guarantees onSync runs one at a time per block, giving us atomic
  // read-modify-write on the bucket across concurrent incoming events.
  onSync: async ({ block: { config } }) => {
    const count = config.count as number;
    const interval = config.interval as number;
    validate(count, interval);

    const { pairs } = await kv.block.list({ keyPrefix: QUEUE_PREFIX });
    const { value: oldTimerId } = await kv.block.get(TIMER_KEY);

    if (pairs.length === 0) {
      // Nothing queued — clear any stale timer state and idle.
      const cleanup: Promise<any>[] = [];
      if (oldTimerId) {
        cleanup.push(timers.unset(oldTimerId), kv.block.delete([TIMER_KEY]));
      }
      await Promise.all(cleanup);
      return { newStatus: "ready" };
    }

    const now = Date.now();
    const { value: stored } = await kv.block.get(BUCKET_KEY);
    const prev: BucketState = stored ?? { tokens: count, lastUpdate: now };

    const tokensPerMs = count / (interval * 1000);
    let tokens = Math.min(
      count,
      prev.tokens + (now - prev.lastUpdate) * tokensPerMs,
    );

    // Drain in arrival order while tokens are available.
    const completed: Promise<void>[] = [];
    const drainedKeys: string[] = [];
    for (const pair of pairs) {
      if (tokens < 1) break;
      const { pendingEventId } = pair.value as QueuedEvent;
      tokens -= 1;
      drainedKeys.push(pair.key);
      completed.push(events.completePending(pendingEventId));
    }

    const remaining = pairs.length - drainedKeys.length;

    // Reschedule timer: cancel old, set new only if events remain. Timer
    // resolution is 1 second so we round up; the bucket will simply have
    // more refill than the bare minimum when we wake.
    const timerOps: Promise<any>[] = [];
    if (oldTimerId) {
      timerOps.push(timers.unset(oldTimerId));
    }
    if (remaining > 0) {
      const need = Math.max(0, 1 - tokens);
      const secondsUntilNextToken = Math.max(
        1,
        Math.ceil((need * interval) / count),
      );
      const newTimerId = await timers.set(secondsUntilNextToken, {});
      timerOps.push(kv.block.set({ key: TIMER_KEY, value: newTimerId }));
    } else if (oldTimerId) {
      timerOps.push(kv.block.delete([TIMER_KEY]));
    }

    await Promise.all([
      kv.block.set({
        key: BUCKET_KEY,
        value: { tokens, lastUpdate: now } satisfies BucketState,
      }),
      ...(drainedKeys.length > 0 ? [kv.block.delete(drainedKeys)] : []),
      ...completed,
      ...timerOps,
    ]);

    return {
      newStatus: "ready",
      customStatusDescription:
        remaining > 0 ? `Queued: ${remaining}` : undefined,
    };
  },

  // Timer just kicks the lifecycle. The actual draining lives in onSync, which
  // is the serialized state-mutation surface.
  onTimer: async () => {
    await lifecycle.sync();
  },

  outputs: {
    default: {
      default: true,
      name: "Throttled",
      description:
        "Triggered for events that have been admitted by the throttle. Body is " +
        "empty; bind downstream blocks to the original upstream block to read " +
        "event data.",
      possiblePrimaryParents: ["default"],
      type: { type: "object", additionalProperties: true },
    },
  },
};

export default throttle;
