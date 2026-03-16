import { AppBlock, events, kv } from "@slflows/sdk/v1";
import { randomUUID } from "node:crypto";

export default {
  name: "For Loop",
  description:
    "A loop block that iterates over a range of values, emitting events for each iteration",
  category: "Control Flow",
  config: {
    start: {
      name: "Start",
      description: "Starting value (inclusive)",
      type: "number",
      required: true,
      default: 0,
    },
    end: {
      name: "End",
      description: "Ending value (exclusive)",
      type: "number",
      required: true,
      default: 10,
    },
    step: {
      name: "Step",
      description: "Step increment",
      type: "number",
      required: false,
      default: 1,
    },
  },
  inputs: {
    default: {
      onEvent: async (input) => {
        const start = input.block.config.start;
        const end = input.block.config.end;
        const step = input.block.config.step;

        if (step === 0) {
          throw new Error("Step cannot be zero");
        }

        // Nothing to iterate if start is already past end
        const wouldIterate = step > 0 ? start < end : start > end;
        if (!wouldIterate) {
          // Loop is complete, emit completion event
          await events.emit(
            {
              iterations: 0,
            },
            {
              outputKey: "default",
            },
          );
          return;
        }

        // Initialize loop state in KV storage
        const loopId = randomUUID();
        const loopKey = `loop_${loopId}`;
        await kv.block.set({
          key: loopKey,
          value: {
            start,
            end,
            step,
            currentIndex: start,
            iterationCount: 1,
            outputs: [],
          },
        });

        // Emit first iteration
        await events.emit(
          {
            index: start,
            loopId: loopId,
          },
          {
            outputKey: "loop",
            echo: true,
          },
        );
      },
    },
    loop: {
      config: {
        continueLoop: {
          name: "Continue Loop",
          description: "Whether to continue the loop iteration",
          type: "boolean",
          required: false,
          default: true,
        },
        outputValue: {
          name: "Output Value",
          description:
            "Optional value to collect from this iteration. All collected values will be included in the final completion event.",
          type: "any",
          required: false,
        },
      },
      onEvent: async (input) => {
        const { continueLoop, outputValue } = input.event.inputConfig;

        const loopId = input.event.echo?.body.loopId;

        if (!loopId) {
          console.warn(
            `Received event ${input.event.id} from outside loop body, ignoring`,
          );
          return;
        }

        const loopKey = `loop_${loopId}`;

        const { value: loopState } = await kv.block.get(loopKey);

        const { start, end, step, currentIndex, iterationCount, outputs } =
          loopState;
        const nextIndex = currentIndex + step;
        const nextIterationCount = iterationCount + 1;
        const nextOutputs =
          outputValue !== undefined ? [...outputs, outputValue] : outputs;

        // Check if we should continue the loop (handles both positive and negative steps)
        const withinRange = step > 0 ? nextIndex < end : nextIndex > end;
        const shouldContinue = (continueLoop ?? true) && withinRange;

        if (shouldContinue) {
          // Update state for next iteration
          await kv.block.set({
            key: loopKey,
            value: {
              start,
              end,
              step,
              currentIndex: nextIndex,
              iterationCount: nextIterationCount,
              outputs: nextOutputs,
            },
          });

          // Emit next iteration
          await events.emit(
            {
              index: nextIndex,
              loopId: loopId,
            },
            {
              outputKey: "loop",
              echo: true,
            },
          );
        } else {
          // Clean up loop state before emitting completion
          await kv.block.delete([loopKey]);

          // Loop is complete, emit completion event
          await events.emit(
            {
              iterations: iterationCount,
              outputs: nextOutputs,
            },
            {
              outputKey: "default",
            },
          );
        }
      },
    },
  },
  outputs: {
    default: {
      default: true,
      type: {
        type: "object",
        properties: {
          iterations: {
            type: "number",
          },
          outputs: {
            type: "array",
            items: { type: "any" },
          },
        },
      },
    },
    loop: {
      type: {
        type: "object",
        properties: {
          index: {
            type: "number",
          },
        },
      },
    },
  },
} satisfies AppBlock;
