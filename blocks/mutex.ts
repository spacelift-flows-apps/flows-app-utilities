import {
  AppBlock,
  EntityLifecycleCallbackOutput,
  events,
  kv,
  lifecycle,
  timers,
} from "@slflows/sdk/v1";

const CURRENT_HOLDER_KEY = "currentHolder";

const clearLock = async (eventId: string): Promise<Boolean> => {
  return await kv.block.set({
    key: CURRENT_HOLDER_KEY,
    value: null,
    ttl: 0,
    lock: { id: eventId },
  });
};

const statusHeld: EntityLifecycleCallbackOutput = {
  newStatus: "ready",
  customStatusDescription: "Held",
};

const mutex: AppBlock = {
  autoconfirm: true,
  name: "Mutex",
  category: "Control",
  description:
    "Ensures only one event can be processed at a time by creating a queue system.\n\n" +
    "How it works:\n" +
    "- Events are queued in arrival order\n" +
    "- Only the first event gets processed immediately\n" +
    "- Others wait their turn until the mutex is released\n\n" +
    "Release mechanisms:\n" +
    "- Automatic timeout: Events are released after the configured timeout period\n" +
    "- Manual release: Use the `release` input to manually release the mutex\n\n" +
    "Use cases: Rate-limited APIs, file operations, database transactions, " +
    "or any resource that can only handle one operation at a time.",

  onSync: async () => {
    // First, detect if the releaser value has changed by comparing the one
    // stored in the KV store with the one provided in the config.
    const { value: currentHolder } = await kv.block.get(CURRENT_HOLDER_KEY);
    if (currentHolder) {
      return statusHeld;
    }

    // Get the next event ID. When we list events, they are sorted by
    // their creation time. We want to release the oldest event first.
    const { pairs } = await kv.block.list({ keyPrefix: "evt:" });

    // It is possible that there are no events waiting for the mutex
    // to be released.
    if (pairs.length === 0) {
      return {
        newStatus: "ready",
        customStatusDescription: "Available",
      };
    }

    // Extract the eldest waiting event from our queue of contenders
    const [
      {
        key,
        value: { timeout, pendingId },
      },
    ] = pairs;
    const eventId = key.split(":")[1];

    await Promise.all([
      timers.block.set(timeout, { inputPayload: eventId }),
      kv.block.setMany([
        { key: CURRENT_HOLDER_KEY, value: eventId, lock: { id: eventId } },
        { key: `evt:${eventId}`, value: "", ttl: 0 }, // Delete the event from the queue.
      ]),
      events.emit(
        { lockId: eventId },
        { complete: pendingId, echo: true, parentEventId: eventId },
      ),
    ]);

    return statusHeld;
  },

  onTimer: async ({ timer: { payload: eventId } }) => {
    if (await clearLock(eventId || "")) {
      await lifecycle.sync();
    }
  },

  inputs: {
    default: {
      name: "Acquire",
      description: "Attempts to acquire the mutex lock for processing",
      config: {
        timeout: {
          name: "Lock timeout",
          type: "number",
          description: "Automatic release timeout in seconds.",
          required: true,
          default: 60,
        },
      },
      onEvent: async ({ event }) => {
        const pendingId = await events.createPending({
          statusDescription: "Waiting for mutex",
        });
        const timeout = event.inputConfig.timeout;

        await kv.block.set({
          key: `evt:${event.id}`,
          value: { pendingId, timeout },
        });
        await lifecycle.sync();
      },
    },
    release: {
      name: "Release",
      description: "Releases the mutex lock",
      config: {},
      onEvent: async ({ event: { echo } }) => {
        if (await clearLock(echo?.body.lockId || "")) {
          await lifecycle.sync();
        }
      },
    },
  },
  outputs: {
    default: {
      possiblePrimaryParents: ["default"],
      type: {
        type: "object",
        properties: {
          lockId: {
            type: "string",
            description:
              "Unique identifier for the event that acquired the mutex",
          },
        },
      },
    },
  },
};

export default mutex;
