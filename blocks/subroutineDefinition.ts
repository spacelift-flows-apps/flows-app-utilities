import { AppBlock, events, kv, messaging } from "@slflows/sdk/v1";
import { randomUUID } from "node:crypto";

const subroutineDefinition: AppBlock = {
  name: "Subroutine definition (deprecated)",
  autoconfirm: true,
  category: "Control",
  description:
    "Deprecated: use the built-in Subroutine Definition block instead. " +
    "Define a subroutine that can be called by Subroutine Call blocks. " +
    "Exports its block ID as a signal for reference.",
  inputs: {
    result: {
      name: "Result",
      description: "Send the result back to the subroutine caller",
      config: {
        value: {
          name: "Result value",
          description: "The result to return to the caller",
          type: "any",
          required: true,
        },
      },
      onEvent: async (input) => {
        const config = input.event.inputConfig;

        if (!input.event.echo) {
          throw new Error("This block should not be called directly");
        }

        const { executionId } = input.event.echo.body;

        const { value } = await kv.block.get(`execution_${executionId}`);

        if (!value) {
          throw new Error("No execution context found");
        }

        await messaging.sendToBlocks({
          body: {
            result: config.value,
            eventId: value.eventId,
            pendingEventId: value.pendingEventId,
            timerId: value.timerId,
          },
          blockIds: [value.blockId],
        });

        await kv.block.delete([`execution_${executionId}`]);
      },
    },
  },
  outputs: {
    onCall: {
      name: "On call",
      description: "Emitted when the subroutine is called",
      type: {
        type: "object",
        properties: {
          input: {
            type: "object",
            additionalProperties: true,
            description: "The payload passed from the caller",
          },
        },
        required: ["input"],
      },
    },
  },
  onSync: async (input) => {
    return {
      newStatus: "ready",
      signalUpdates: {
        subroutineId: input.block.id,
      },
    };
  },
  onInternalMessage: async (input) => {
    const { eventId, payload, blockId, pendingEventId, timerId } =
      input.message.body;

    const executionId = randomUUID();

    await kv.block.set({
      key: `execution_${executionId}`,
      value: { eventId, blockId, pendingEventId, timerId },
    });

    await events.emit(
      { executionId, input: payload },
      { echo: true, secondaryParentEventIds: [eventId] },
    );
  },
  signals: {
    subroutineId: {
      name: "Subroutine ID",
      description: "The unique ID of this subroutine block",
    },
  },
};

export default subroutineDefinition;
