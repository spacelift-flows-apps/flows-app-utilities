import { AppBlock, events, messaging, timers } from "@slflows/sdk/v1";

const subroutineCall: AppBlock = {
  name: "Subroutine call",
  category: "Control",
  description:
    "Call a subroutine defined by a Subroutine Definition block. " +
    "Pass a payload and receive the result.",
  inputs: {
    default: {
      name: "Call subroutine",
      description: "Execute the subroutine with the provided payload",
      config: {
        subroutineId: {
          name: "Subroutine ID",
          description: "Reference to the subroutine definition block",
          type: "string",
          required: true,
        },
        payload: {
          name: "Payload",
          description: "JSON payload to pass to the subroutine",
          type: "any",
          required: true,
        },
        timeoutSeconds: {
          name: "Timeout (seconds)",
          description:
            "Maximum time to wait for subroutine to complete before timing out",
          type: "number",
          required: false,
          default: 120,
        },
      },
      onEvent: async (input) => {
        const config = input.event.inputConfig;
        const { subroutineId, payload, timeoutSeconds } = config;

        const eventId = input.event.id;

        const pendingEventId = await events.createPending({
          statusDescription: "Calling subroutine...",
        });

        // Set a timeout timer
        const timerId = await timers.block.set(timeoutSeconds, {
          pendingEventId,
          inputPayload: { eventId },
        });

        // Send message to the subroutine definition block
        await messaging.sendToBlocks({
          body: {
            blockId: input.block.id,
            eventId, // Original event ID for correlation
            payload,
            pendingEventId, // To resolve when subroutine completes
            timerId, // To clear the timer when done
          },
          blockIds: [subroutineId],
        });
      },
    },
  },
  outputs: {
    result: {
      name: "Result",
      description: "The result returned by the subroutine",
      default: true,
      possiblePrimaryParents: ["default"],
      type: {
        type: "object",
        properties: {
          value: {
            description: "The result value from the subroutine",
            additionalProperties: true,
          },
        },
        required: ["value"],
      },
    },
    timeout: {
      name: "Timeout",
      description: "Emitted when the subroutine call times out",
      secondary: true,
      type: {
        type: "object",
        properties: {
          eventId: {
            type: "string",
            description: "The original event ID that timed out",
          },
        },
        required: ["eventId"],
      },
    },
  },

  onTimer: async (input) => {
    const { pendingEvent, payload } = input.timer;

    await Promise.all([
      events.emit(
        {},
        {
          complete: pendingEvent!.id,
          outputKey: "timeout",
          parentEventId: payload.eventId,
        },
      ),
    ]);
  },

  onInternalMessage: async (input) => {
    const { result, eventId, pendingEventId, timerId } = input.message.body;
    await Promise.all([
      timers.block.unset(timerId),

      events.emit(
        { value: result },
        { parentEventId: eventId, complete: pendingEventId },
      ),
    ]);
  },
};

export default subroutineCall;
