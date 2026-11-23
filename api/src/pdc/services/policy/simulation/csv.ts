export interface SimulationRow {
  operator_id: number;
  operator_journey_id: string;
  datetime: Date;
  start_arr: string;
  end_arr: string;
  distance: number;
  current_amount?: number;
  current_status?: string;
  simulated_stateless_amount?: number;
  simulated_final_amount?: number;
  rejection_reason?: string;
  simulated_final_status?: string;
}

// amounts are in cents, as stored in policy.incentives
export const CSV_COLUMNS = [
  "operator_id",
  "operator_journey_id",
  "datetime",
  "start_arr",
  "end_arr",
  "distance",
  "current_amount",
  "current_status",
  "simulated_stateless_amount",
  "simulated_final_amount",
  "rejection_reason",
  "simulated_final_status",
] as const satisfies ReadonlyArray<keyof SimulationRow>;

function cell(value: unknown): string {
  if (value === undefined || value === null) return "";
  const s = value instanceof Date ? value.toISOString() : String(value);
  return /[",\n\r]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
}

export function toCsvLine(row: SimulationRow): string {
  return CSV_COLUMNS.map((c) => cell(row[c])).join(",");
}

export function toCsv(rows: SimulationRow[]): string {
  return [CSV_COLUMNS.join(","), ...rows.map(toCsvLine)].join("\n") + "\n";
}
