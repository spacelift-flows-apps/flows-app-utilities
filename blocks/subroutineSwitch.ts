import { AppBlock, events, messaging, timers } from "@slflows/sdk/v1";

interface SwitchCase {
  condition: unknown;
  subroutineId: string;
}

const subroutineSwitch: AppBlock = {
  name: "Subroutine switch",
  category: "Control",
  description:
    "Evaluate cases in order and call the first matching subroutine — " +
    "like a switch/case statement. Each case has a condition that the " +
    "platform resolves to a value; only a strict `true` triggers a match.\n\n" +
    "If nothing matches, the default subroutine runs (if configured), " +
    "otherwise the noMatch output fires.",
  inputs: {
    default: {
      name: "Evaluate",
      description:
        "Trigger evaluation of all configured cases. " +
        "The payload is forwarded to the matched subroutine.",
      config: {
        cases: {
          name: "Cases",
          description:
            "Ordered list of cases. The first case whose condition resolves " +
            "to exactly `true` wins. Remaining cases are not evaluated.",
          type: {
            type: "array",
            items: {
              type: "object",
              properties: {
                condition: {
                  description:
                    "Must resolve to exactly `true` for this case to match. " +
                    "Wire to an expression or previous block output.",
                },
                subroutineId: {
                  type: "string",
                  description:
                    "Subroutine definition block to call when this case matches.",
                },
              },
              required: ["condition", "subroutineId"],
            },
          },
          required: true,
          default: [],
        },
        defaultSubroutineId: {
          name: "Default subroutine ID",
          description:
            "Subroutine to call if no cases match. " +
            "If not set and nothing matches, the noMatch output fires instead.",
          type: "string",
          required: false,
        },
        payload: {
          name: "Payload",
          description: "Data to pass to the matched subroutine",
          type: "any",
          required: false,
        },
        timeoutSeconds: {
          name: "Timeout (seconds)",
          description:
            "Maximum time to wait for the subroutine to complete before timing out",
          type: "number",
          required: false,
          default: 120,
        },
      },
      onEvent: async (input) => {
        const inputConfig = input.event.inputConfig;
        const eventId = input.event.id;

        const timeoutSeconds: number = inputConfig.timeoutSeconds ?? 120;
        const payload: unknown = inputConfig.payload;
        const defaultSubroutineId: string | undefined =
          inputConfig.defaultSubroutineId;

        const rawCases: SwitchCase[] = Array.isArray(inputConfig.cases)
          ? inputConfig.cases
          : [];

        // Find first matching case (strict true)
        let matchedId: string | undefined;
        for (const c of rawCases) {
          if (c.subroutineId && c.condition === true) {
            matchedId = c.subroutineId;
            break;
          }
        }

        // Fall back to default
        if (!matchedId) {
          matchedId = defaultSubroutineId;
        }

        // Nothing matched and no default — emit noMatch and bail
        if (!matchedId) {
          await events.emit(
            { evaluatedCases: rawCases.length },
            { outputKey: "noMatch", parentEventId: eventId },
          );
          return;
        }

        const pendingEventId = await events.createPending({
          statusDescription: "Calling subroutine...",
        });

        const timerId = await timers.set(timeoutSeconds, {
          pendingEventId,
          inputPayload: { eventId },
        });

        await messaging.sendToBlocks({
          body: {
            blockId: input.block.id,
            eventId,
            payload,
            pendingEventId,
            timerId,
          },
          blockIds: [matchedId],
        });
      },
    },
  },
  outputs: {
    result: {
      name: "Result",
      description: "Emitted when the matched subroutine completes",
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
    noMatch: {
      name: "No match",
      description:
        "Emitted when no conditions evaluated to true and no default subroutine is configured",
      secondary: true,
      type: {
        type: "object",
        properties: {
          evaluatedCases: {
            type: "number",
            description: "Number of cases that were evaluated",
          },
        },
        required: ["evaluatedCases"],
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

    await events.emit(
      {},
      {
        complete: pendingEvent!.id,
        outputKey: "timeout",
        parentEventId: payload.eventId,
      },
    );
  },

  onInternalMessage: async (input) => {
    const { result, eventId, pendingEventId, timerId } = input.message.body;

    await Promise.all([
      timers.unset(timerId),
      events.emit(
        { value: result },
        { parentEventId: eventId, complete: pendingEventId },
      ),
    ]);
  },
};

export default subroutineSwitch;
