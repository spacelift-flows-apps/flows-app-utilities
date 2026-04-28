import { AppBlock, events, kv } from "@slflows/sdk/v1";

const BUCKET_KEY = "bucket";

interface BucketState {
  tokens: number;
  lastUpdate: number;
}

const rateLimit: AppBlock = {
  name: "Rate Limit",
  category: "Control",
  description:
    "Classifies events as `allowed` or `excess` based on a configured rate, " +
    "without delaying or dropping them.\n\n" +
    "How it works:\n" +
    "- A token bucket holds up to `count` tokens and refills continuously to " +
    "full over `interval` seconds\n" +
    "- Each incoming event consumes one token if available\n" +
    "- Events that find a token trigger the `allowed` (default) output\n" +
    "- Events that arrive when the bucket is empty trigger the `excess` " +
    "output (no token consumed)\n\n" +
    "Burst behavior:\n" +
    "- The bucket starts full, so the first `count` events pass through " +
    "instantly even after long idle periods\n" +
    "- Steady-state throughput on the `allowed` output is bounded by " +
    "`count / interval` events per second\n\n" +
    "Use cases:\n" +
    "- Sampling: only act on a fraction of an event stream and log the rest\n" +
    "- Spam control: forward the first N events per minute, route the rest to a " +
    "warning path\n" +
    "- Cost control: cap calls to a paid downstream API and shed the overflow\n\n" +
    "Companion: use the Throttle block instead if you want excess events queued " +
    "and emitted later rather than diverted to a separate output.",

  config: {
    count: {
      name: "Count",
      description:
        "Maximum number of events allowed through per `interval` seconds " +
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
      onEvent: async (input) => {
        const count = input.block.config.count as number;
        const interval = input.block.config.interval as number;

        if (!(count > 0) || !(interval > 0)) {
          throw new Error(
            `Rate Limit requires count > 0 and interval > 0 (got count=${count}, interval=${interval})`,
          );
        }

        const now = Date.now();
        const { value: stored } = await kv.block.get(BUCKET_KEY);
        const prev: BucketState = stored ?? { tokens: count, lastUpdate: now };

        const tokensPerMs = count / (interval * 1000);
        const refilled = Math.min(
          count,
          prev.tokens + (now - prev.lastUpdate) * tokensPerMs,
        );

        const allowed = refilled >= 1;
        const newTokens = allowed ? refilled - 1 : refilled;

        await Promise.all([
          kv.block.set({
            key: BUCKET_KEY,
            value: { tokens: newTokens, lastUpdate: now } satisfies BucketState,
          }),
          events.emit(
            {},
            {
              outputKey: allowed ? "default" : "excess",
            },
          ),
        ]);
      },
    },
  },

  outputs: {
    default: {
      default: true,
      name: "Allowed",
      description:
        "Triggered for events that consumed a token. Body is empty; bind " +
        "downstream blocks to the original upstream block to read event data.",
      possiblePrimaryParents: ["default"],
      type: { type: "object", additionalProperties: true },
    },
    excess: {
      name: "Excess",
      description:
        "Triggered for events that arrived when the bucket was empty. No " +
        "token was consumed. Body is empty.\n\n" +
        "Connect this to logging, alerting, or a fallback path. Leave it " +
        "unconnected to drop excess events silently.",
      secondary: true,
      possiblePrimaryParents: ["default"],
      type: { type: "object", additionalProperties: true },
    },
  },
};

export default rateLimit;
