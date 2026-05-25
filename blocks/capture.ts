import { AppBlock, kv, lifecycle, timers } from "@slflows/sdk/v1";

const KV_KEYS = {
  VALUE: "value",
  EXPIRES_AT: "expiresAt",
  EVENT_ID: "eventId",
};

const capture: AppBlock = {
  name: "Capture",
  category: "Data",
  description:
    "Converts events into persistent signals that other blocks can observe",
  config: {
    defaultValue: {
      name: "Default value",
      description:
        "The starting value for your signal before any events are captured, " +
        "and the value it returns to when the capture expires.\n\n" +
        "What to set:\n" +
        "- Choose a value that makes sense when nothing has happened yet\n" +
        "- This value is used when the timeout resets the signal\n" +
        "- Should match the type of data you expect to capture",
      type: "any",
      required: false,
      default: {},
    },
  },
  inputs: {
    default: {
      config: {
        value: {
          name: "Value to export",
          description:
            "The specific data you want to extract from the event and turn into a signal.",
          type: "any",
          required: true,
          default: {},
        },
        timeoutSeconds: {
          name: "Timeout in seconds",
          description:
            "How long to keep the captured value before automatically resetting to the default.\n\n" +
            "Why use a timeout:\n" +
            "- Prevents old signals from sticking around forever\n" +
            "- Great for temporary status indicators\n" +
            "- Signal automatically returns to default value after timeout\n\n" +
            "Good for:\n" +
            '- Temporary status messages ("processing" → "idle")\n' +
            "- Rate limiting signals that should reset\n" +
            "- Error states that should clear automatically\n" +
            "- One-time triggers that need to reset\n\n" +
            "Leave empty for permanent signals that stay until overwritten.",
          type: "number",
          required: false,
        },
      },
      onEvent: async (input) => {
        try {
          const { value, timeoutSeconds } = input.event.inputConfig;
          const eventId = input.event.id;
          let expiresAt: number | null = null;

          // Handle timeout if provided
          if (timeoutSeconds !== undefined && timeoutSeconds > 0) {
            // Calculate expiration time
            expiresAt = Date.now() + timeoutSeconds * 1000;

            // Set a new timer to reset value when timeout expires
            await timers.block.set(timeoutSeconds, {
              description: "Reset captured value to default",
              inputPayload: { eventId },
            });
          }

          // Store the value, expiration time, and event ID in KV store
          await kv.block.setMany([
            { key: KV_KEYS.VALUE, value },
            { key: KV_KEYS.EXPIRES_AT, value: expiresAt },
            { key: KV_KEYS.EVENT_ID, value: eventId },
          ]);

          // Trigger sync to update lifecycle signals
          await lifecycle.sync();
        } catch (error) {
          console.error("Error processing capture event:", error);
        }
      },
    },
  },
  onSync: async (input) => {
    try {
      const now = Date.now();
      const defaultValue = input.block.config.defaultValue;

      // Retrieve the captured value, expiration time, and event ID from KV store
      const [{ value }, { value: expiresAt }, { value: eventId }] =
        await kv.block.getMany([
          KV_KEYS.VALUE,
          KV_KEYS.EXPIRES_AT,
          KV_KEYS.EVENT_ID,
        ]);

      // Helper function to create consistent export updates
      const createExportUpdates = (
        value: any,
        expiresAt?: number | null,
        eventId?: string | null,
      ) => ({
        value,
        updatedAt: now,
        expiresAt,
        updatedBy: eventId || "expiration",
      });

      // Handle expired value
      if (expiresAt && now > expiresAt) {
        // Reset KV store with default values
        await kv.block.setMany([
          { key: KV_KEYS.VALUE, value: defaultValue },
          { key: KV_KEYS.EXPIRES_AT, value: null },
          { key: KV_KEYS.EVENT_ID, value: null },
        ]);

        return {
          signalUpdates: createExportUpdates(defaultValue, null, null),
          newStatus: "ready",
        };
      }

      // Handle no value case
      if (!value) {
        return {
          signalUpdates: createExportUpdates(defaultValue, null, null),
          newStatus: "ready",
        };
      }

      // Normal case - return current values
      return {
        signalUpdates: createExportUpdates(value, expiresAt, eventId),
        newStatus: "ready",
      };
    } catch (error) {
      console.error("Error in capture sync:", error);
      return {
        newStatus: "ready",
        signalUpdates: {
          value: input.block.config.defaultValue,
          updatedAt: null,
          expiresAt: null,
          updatedBy: null,
        },
      };
    }
  },

  onDrain: async () => {
    return { newStatus: "drained" };
  },
  onTimer: async (input) => {
    try {
      // Get the current event ID from KV store for more accurate comparison
      const { value: currentEventId } = await kv.block.get(KV_KEYS.EVENT_ID);
      const { block } = input;
      const defaultValue = block.config.defaultValue;

      // Only reset if the timer's event ID matches the current one
      if (
        input.timer.payload &&
        input.timer.payload.eventId === currentEventId
      ) {
        // Reset all KV values to defaults
        await kv.block.setMany([
          { key: KV_KEYS.VALUE, value: defaultValue },
          { key: KV_KEYS.EXPIRES_AT, value: null },
          { key: KV_KEYS.EVENT_ID, value: null },
        ]);

        await lifecycle.sync();
      }
    } catch (error) {
      console.error("Error in timer handler:", error);
    }
  },
  signals: {
    value: {
      name: "Value",
      description:
        "The captured signal value - this is what other blocks observe and react to.\n\n" +
        "Main signal to connect:\n" +
        "- Connect this to other blocks like observe blocks or conditional logic\n" +
        "- Updates whenever a new event is captured\n" +
        "- Triggers connected blocks when the value changes\n" +
        "- Automatically reverts to default value when timeout expires",
    },
    updatedAt: {
      name: "Last updated timestamp",
      description:
        "When the signal was last updated (Unix milliseconds timestamp).\n\n" +
        "Useful for:\n" +
        "- Checking how fresh your captured data is\n" +
        "- Creating time-based logic\n" +
        "- Debugging when signals were updated\n" +
        "- Building time-based triggers",
    },
    updatedBy: {
      name: "Updated by",
      description:
        "The ID of the event that created this signal (null if using default value).\n\n" +
        "Useful for:\n" +
        "- Tracking which event produced the current signal\n" +
        "- Debugging where signals came from\n" +
        "- Correlating signals with specific events\n" +
        "- Creating audit trails of signal changes",
    },
    expiresAt: {
      name: "Expires at",
      description:
        "When the signal will automatically reset (Unix milliseconds timestamp, null if no expiration).\n\n" +
        "Useful for:\n" +
        "- Monitoring how long until the signal expires\n" +
        "- Creating cleanup logic\n" +
        "- Building time-based state machines\n" +
        "- Debugging timeout behavior",
    },
  },
};

export default capture;
