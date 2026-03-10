import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { flow, State, StaticMigrable } from "../interfaces/index.ts";
import { MemoryStateManager } from "./index.ts";

describe("Memory", () => {
  const migrable1 = Symbol("1") as unknown as StaticMigrable;
  const migrable2 = Symbol("2") as unknown as StaticMigrable;
  const migrable3 = Symbol("3") as unknown as StaticMigrable;
  const migrable4 = Symbol("4") as unknown as StaticMigrable;
  const migrable5 = Symbol("5") as unknown as StaticMigrable;

  const state = new MemoryStateManager(new Set([migrable1]));

  it("Should get done", () => {
    const result = state.get();
    assertEquals(result, new Set([migrable1]));
  });

  it("Should plan", () => {
    state.plan([migrable2, migrable3, migrable4]);
    const result = state.get(State.Planned);
    assertEquals(result, new Set([migrable2, migrable3, migrable4]));
  });

  it("Should get todo", () => {
    const iter = state.todo();
    let done = false;
    let i = 0;
    do {
      const { value, done: isDone } = iter.next();
      if (isDone) {
        done = true;
      }
      if (value) {
        i += 1;
        const [migrableCtor, migrableState] = value;
        switch (i) {
          case 1:
            assertEquals(migrableCtor, migrable2);
            assertEquals(migrableState, State.Planned);
            state.plan([migrable5], migrable2);
            state.set(
              migrableCtor,
              flow[flow.indexOf(migrableState) + 1],
            );
            break;
          case 2:
            assertEquals(migrableCtor, migrable5);
            assertEquals(migrableState, State.Planned);
            state.set(
              migrableCtor,
              flow[flow.indexOf(migrableState) + 1],
            );
            break;
          case 3:
            assertEquals(migrableCtor, migrable3);
            assertEquals(migrableState, State.Planned);
            state.set(
              migrableCtor,
              flow[flow.indexOf(migrableState) + 1],
            );
            break;
          case 4:
            assertEquals(migrableCtor, migrable4);
            assertEquals(migrableState, State.Planned);
            state.set(
              migrableCtor,
              flow[flow.indexOf(migrableState) + 1],
            );
            break;
          case 5:
            assertEquals(migrableCtor, migrable5);
            assertEquals(migrableState, State.Validated);
            state.set(
              migrableCtor,
              flow[flow.indexOf(migrableState) + 1],
            );
            break;
          case 6:
            assertEquals(migrableCtor, migrable2);
            assertEquals(migrableState, State.Validated);
            state.set(
              migrableCtor,
              flow[flow.indexOf(migrableState) + 1],
            );
            break;
          case 7:
            assertEquals(migrableCtor, migrable3);
            assertEquals(migrableState, State.Validated);
            state.set(
              migrableCtor,
              flow[flow.indexOf(migrableState) + 1],
            );
            break;
          case 8:
            assertEquals(migrableCtor, migrable4);
            assertEquals(migrableState, State.Validated);
            state.set(
              migrableCtor,
              flow[flow.indexOf(migrableState) + 1],
            );
            break;
          case 9:
            assertEquals(migrableCtor, migrable5);
            assertEquals(migrableState, State.Created);
            state.set(
              migrableCtor,
              flow[flow.indexOf(migrableState) + 1],
            );
            break;
          case 10:
            assertEquals(migrableCtor, migrable2);
            assertEquals(migrableState, State.Created);
            state.set(
              migrableCtor,
              flow[flow.indexOf(migrableState) + 1],
            );
            break;
          case 11:
            assertEquals(migrableCtor, migrable3);
            assertEquals(migrableState, State.Created);
            state.set(migrable3, State.Failed);
            state.unplanAfter(migrableCtor);
            break;
          default:
            state.set(
              migrableCtor,
              flow[flow.indexOf(migrableState) + 1],
            );
            break;
        }
      }
    } while (!done);
  });
});
