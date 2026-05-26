import { AppBlock, events, kv, timers } from "@slflows/sdk/v1";

const timeout: AppBlock = {
  name: "Timeout",
  category: "Control",
  description:
    "Creates cancellable delayed events that emit after a specified time period.\n\n" +
    "How it works:\n" +
    "1. Start timer: Send event with delay → receives timer ID immediately\n" +
    "2. Wait period: Timer runs in background for specified duration\n" +
    "3. Timeout event: After delay, emits event on timeouts output\n" +
    "4. Optional cancel: Use timer ID to cancel before timeout occurs\n\n" +
    "Key features:\n" +
    "- Immediate response: Start input emits timer ID right away\n" +
    "- Delayed execution: Actual timeout event comes later\n" +
    "- Cancellable: Stop timers before they fire using the timer ID\n" +
    "- Non-blocking: Multiple timers can run simultaneously\n\n" +
    "Common uses:\n" +
    "- User interaction timeouts: Cancel operations if no user response\n" +
    "- Rate limiting: Enforce minimum delays between actions\n" +
    "- Batch processing: Collect events for a period then process\n" +
    "- Circuit breakers: Reset error states after cooldown periods\n" +
    "- Session expiry: Auto-logout users after inactivity\n" +
    "- Reminder systems: Send notifications after delays\n" +
    "- Cleanup tasks: Trigger maintenance after time periods",

  inputs: {
    start: {
      name: "Start timer",
      description:
        "Starts a new timer - emits timer ID immediately, timeout event comes later.\n\n" +
        "Immediate response:\n" +
        "- Emits timer ID on default output right away\n" +
        "- Use this ID to cancel the timer if needed\n" +
        "- Timer runs in background for the specified duration\n\n" +
        "Delayed event:\n" +
        "- After the delay, emits timeout event on timeouts output\n" +
        "- Contains original timer configuration\n" +
        "- Triggers downstream timeout handling logic",
      config: {
        seconds: {
          name: "Delay (seconds)",
          description:
            "Delay duration in seconds before the timeout event is emitted.\n\n" +
            "Timing behavior:\n" +
            "- Timer starts immediately when event is received\n" +
            "- Timeout event emitted after this exact duration\n" +
            "- Can be cancelled anytime before the timeout occurs\n" +
            "- Multiple timers with different delays can run simultaneously",
          type: "number",
          required: true,
          default: 10,
        },
      },
      onEvent: async ({ event }) => {
        const seconds = event.inputConfig.seconds;
        const uuid = crypto.randomUUID();

        // Create the pending event for visibility
        const pendingEventId = await events.createPending({
          event: { startedAt: Date.now(), seconds, timerId: uuid },
          outputKey: "timeouts",
          statusDescription: `Pending timeout event for ${seconds} seconds`,
        });

        // Set the timer, grab its *internal* ID.
        const timerId = await timers.block.set(seconds, {
          inputPayload: uuid,
          pendingEventId,
        });

        await Promise.all([
          kv.block.set({ key: uuid, value: { pendingEventId, timerId } }),
          events.emit({ timerId: uuid }),
        ]);
      },
    },

    cancel: {
      name: "Cancel timer",
      description:
        "Cancels a running timer before it emits a timeout event.\n\n" +
        "Cancellation behavior:\n" +
        "- Prevents the timeout event from being emitted\n" +
        "- Cleans up timer resources immediately\n" +
        "- Safe to call even if timer already fired or doesn't exist\n" +
        "- No output events emitted when cancelling\n\n" +
        "Use cases:\n" +
        "- User completes action before timeout (cancel the timeout)\n" +
        "- Error conditions that invalidate the timer\n" +
        "- System shutdown or cleanup procedures\n" +
        "- Changing timeout requirements (cancel old, start new)",
      config: {
        timerId: {
          name: "Timer ID",
          description:
            "**Timer identifier** received from the start timer output.\n\n**Usage:**\n- Connect this to the `timerId` field from the default output\n- Use the exact ID value returned when the timer was started\n- IDs are unique for each timer instance\n\n**Pattern:**\n1. Start timer → capture `timerId` from default output\n2. Store timer ID (in capture block or event data)\n3. Send timer ID to cancel input when needed\n\n**Examples:**\n- Store in capture block: connect start output to capture, then use captured signal\n- Pass through events: include timer ID in event payload for later cancellation\n- Conditional cancellation: use timer ID in logic to decide when to cancel",
          type: "string",
          required: true,
        },
      },
      onEvent: async ({
        event: {
          inputConfig: { timerId: uuid },
        },
      }) => {
        const {
          value: { timerId, pendingEventId },
        } = await kv.block.get(uuid);

        await Promise.all([
          timers.block.unset(timerId),
          events.cancelPending(pendingEventId, "Timer cancelled by user"),
          kv.block.delete([uuid]),
        ]);
      },
    },
  },

  onTimer: async ({ timer: { payload: uuid } }) => {
    const {
      value: { pendingEventId },
    } = await kv.block.get(uuid);

    await Promise.all([
      events.completePending(pendingEventId),
      kv.block.delete([uuid]),
    ]);
  },

  outputs: {
    default: {
      default: true,
      name: "Timer started",
      description:
        "**Timer ID emitted immediately** when a timer is started - use for cancellation.",
      type: {
        type: "object",
        properties: {
          timerId: {
            type: "string",
            description:
              "**Unique timer identifier** for cancellation.\n\n**Usage:**\n- Store this ID to cancel the timer later\n- Connect to capture blocks to persist the ID\n- Pass through event chains for conditional cancellation\n- Use in cancel input to stop the timer before timeout\n\n**Characteristics:**\n- Unique for each timer started\n- Required for cancellation operations\n- Remains valid until timer fires or is cancelled",
          },
        },
        required: ["timerId"],
      },
    },

    timeouts: {
      name: "Timeout events",
      description:
        "**Delayed timeout events** emitted after the specified delay period has elapsed.\n\n**Event timing:**\n- Emitted exactly after the delay specified when starting the timer\n- Only fires if the timer wasn't cancelled\n- Contains original timer configuration for context\n\n**Usage patterns:**\n- **Timeout handling**: Process events that should happen after delays\n- **Cleanup operations**: Trigger maintenance after time periods\n- **User interaction timeouts**: Handle cases where users don't respond\n- **Rate limiting**: Allow actions again after cooldown periods\n- **Batch processing**: Process collected events after waiting period\n\n**Note:** This output only fires for timers that complete naturally (not cancelled).",
      secondary: true,
      type: {
        type: "object",
        properties: {
          startedAt: {
            type: "number",
            description:
              "**Timestamp when timer was started** (Unix milliseconds) - useful for calculating actual elapsed time.",
          },
          seconds: {
            type: "number",
            description:
              "**Original delay duration** in seconds that was configured when the timer started.",
          },
          timerId: {
            type: "string",
            description:
              "**Timer ID** that identifies which timer fired - matches the ID from timer started output.",
          },
        },
        required: ["startedAt", "seconds", "timerId"],
      },
    },
  },
};

export default timeout;
