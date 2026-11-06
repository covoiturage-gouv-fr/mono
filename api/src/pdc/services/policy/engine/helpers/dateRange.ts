/**
 * Generate a list of dates between two dates
 *
 * @example
 * const boosterDates = [
 *  ...dateRange("2022-01-01", "2022-01-03"),
 * ];
 *
 * @param dates
 * @returns
 */
export function dateRange(...dates: Array<Date | string>): string[] {
  if (dates.length === 0) {
    throw new Error("At least one date is required");
  }

  const [first, ...rest] = castAndSort(dates);
  const last = rest.pop();

  return last ? fill(first, last) : [format(first)];
}

function castAndSort(range: Array<Date | string>): Set<Date> {
  return range
    .map((d) => {
      const date = new Date(typeof d === "string" ? d : d.getTime());
      if (Number.isNaN(date.getTime())) {
        throw new Error(`Invalid Date`);
      }
      // UTC: "YYYY-MM-DD" is parsed as UTC midnight, local time would shift the day
      date.setUTCHours(0, 0, 0, 0);
      return date;
    })
    .sort((a, b) => a.getTime() - b.getTime())
    .reduce((acc, d) => {
      acc.add(d);
      return acc;
    }, new Set<Date>());
}

function format(d: Date): string {
  const year = d.getUTCFullYear();
  const month = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function fill(first: Date, last: Date): Array<string> {
  const dates = new Set<string>();
  const current = new Date(first);
  while (current <= last) {
    dates.add(format(current));
    current.setUTCDate(current.getUTCDate() + 1);
  }

  return [...dates];
}
