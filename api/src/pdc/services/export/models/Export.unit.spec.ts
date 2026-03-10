import { assert } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { ContextType } from "@/ilos/common/index.ts";
import { Export, ExportTarget } from "./Export.ts";

describe("Export", () => {
  function context(user: Required<ContextType>["call"]["user"]): ContextType {
    return {
      channel: { service: "test" },
      call: { user: { ...user, _id: 1 } },
    };
  }

  it("Export.setTarget() defaults to DATAGOUV", () => {
    assert(Export.target(context({})), ExportTarget.DATAGOUV);
    assert(Export.target(context({}), null), ExportTarget.DATAGOUV);
    assert(Export.target(context({}), undefined), ExportTarget.DATAGOUV);
    assert(
      Export.target({ channel: { service: "missing_user" } }),
      ExportTarget.DATAGOUV,
    );
  });

  it("Export.setTarget() sets to territory when valid", () => {
    assert(Export.target(context({ territory_id: 1 })), ExportTarget.TERRITORY);
    assert(
      Export.target(context({ territory_id: null })),
      ExportTarget.DATAGOUV,
    );
    assert(
      Export.target(context({ territory_id: undefined })),
      ExportTarget.DATAGOUV,
    );
    assert(
      Export.target(context({ territory_id: "asdasd" })),
      ExportTarget.DATAGOUV,
    );
  });

  it("Export.setTarget() sets to operator when valid", () => {
    assert(Export.target(context({ operator_id: 1 })), ExportTarget.OPERATOR);
    assert(
      Export.target(context({ operator_id: null })),
      ExportTarget.DATAGOUV,
    );
    assert(
      Export.target(context({ operator_id: undefined })),
      ExportTarget.DATAGOUV,
    );
    assert(
      Export.target(context({ operator_id: "asdasd" })),
      ExportTarget.DATAGOUV,
    );
  });
});
