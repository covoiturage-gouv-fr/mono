import { assertEquals, it } from "@/dev_deps.ts";
import { getTzFromLon } from "./geo.helper.ts";

it("getTzFromLong", () => {
  assertEquals(getTzFromLon(2.3522), "Europe/Paris");
  assertEquals(getTzFromLon(-61.551), "America/Guadeloupe");
  assertEquals(getTzFromLon(55.523), "Indian/Reunion");
});
