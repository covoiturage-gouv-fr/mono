import { assertEquals } from "dep:assert";
import { differenceInHours } from "@/lib/date/index.ts";

Deno.test("should return difference in hours", () => {
  const created_at = new Date("2024-10-24 06:37:09");
  const start_datetime = new Date("2024-10-23 05:00:47");
  const result = differenceInHours(created_at, start_datetime);
  assertEquals(result, 25.60611111111111);
});
