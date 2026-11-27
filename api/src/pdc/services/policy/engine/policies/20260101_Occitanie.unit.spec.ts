import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { CarpoolInterface, OperatorsEnum, TerritoryCodeEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { Occitanie20262027 as Handler } from "./20260101_Occitanie.ts";
import { occitanie2026Data } from "./20260101_Occitanie.data.ts";

const defaultPosition = {
  arr: "46240",
  com: "46240",
  aom: "200053791",
  epci: "200066371",
  dep: "46",
  reg: "76",
  country: "XXXXX",
  reseau: 465,
};
const defaultLat = 48.72565703413325;
const defaultLon = 2.261827843187402;

const defaultCarpool: CarpoolInterface = {
  operator_trip_id: uuidV4(),
  passenger_identity_key: uuidV4(),
  driver_identity_key: uuidV4(),
  operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2026-02-02"), // Monday
  seats: 1,
  distance: 5_000,
  operator_journey_id: uuidV4(),
  operator_id: 1,
  start: { ...defaultPosition },
  end: { ...defaultPosition },
  start_lat: defaultLat,
  start_lon: defaultLon,
  end_lat: defaultLat,
  end_lon: defaultLon,
  driver_revenue: 10,
  passenger_contribution: 50, // minimum 0.5€
};

const process = makeProcessHelper(defaultCarpool);

describe("Occitanie 2026 - Exclusions", () => {
  it("unsupported operator", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        // Non listed operator will fail
        { operator_uuid: OperatorsEnum.BLABLACAR, datetime: new Date("2026-02-02") },

        // Supported operator but before starting date
        { operator_uuid: OperatorsEnum.BLABLACAR_DAILY, datetime: new Date("2025-02-02") },
      ],
    }, { incentive: [0, 0] }));

  it("distance below minimum", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ distance: 1_999 }],
    }, { incentive: [0] }));

  // The campaign has different maximum distances based on areas
  it("distance above maximum", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        // regular area : 30 km max
        { distance: 31_000 },

        // inside specific area : 50 km max
        { distance: 31_000, start: { ...defaultCarpool.start, arr: occitanie2026Data.areas[0] } }, // ok
        { distance: 51_000, start: { ...defaultCarpool.start, arr: occitanie2026Data.areas[0] } }, // ko
      ],
    }, {
      incentive: [0, 200, 0],
    }));

  // trips must start AND end within Occitanie region
  it("trip outside AOM", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { start: { ...defaultPosition, aom: "not_ok" }, end: { ...defaultPosition, aom: "not_ok" } },
        { start: { ...defaultPosition, aom: "not_ok" } },
        { end: { ...defaultPosition, aom: "not_ok" } },
      ],
    }, { incentive: [0, 0, 0] }));

  // Trips from within the same inner AOM are rejected
  it("trip within same inner AOM", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{
        start: { ...defaultPosition, aom: occitanie2026Data.inner[0] },
        end: { ...defaultPosition, aom: occitanie2026Data.inner[0] },
      }],
    }, { incentive: [0] }));

  it("trip within different inner AOM (ok)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        // Grand-Cahors - CA Grand Montauban
        {
          start: { ...defaultPosition, aom: occitanie2026Data.innerByName("CA du Grand Cahors") },
          end: { ...defaultPosition, aom: occitanie2026Data.innerByName("CA Grand Montauban") },
        },
        // Occitanie - Grand-Cahors
        {
          start: { ...defaultPosition, aom: occitanie2026Data.region[TerritoryCodeEnum.Mobility] },
          end: { ...defaultPosition, aom: occitanie2026Data.innerByName("CA du Grand Cahors") },
        },
      ],
    }, { incentive: [150, 150] }));

  it("trip outside the region", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { start: { ...defaultPosition, reg: "84" } },
        { end: { ...defaultPosition, reg: "84" } },
      ],
    }, { incentive: [0, 0] }));

  // Trips starting or ending in a bassin can start/end outside the Region
  it("trip in bassin but other end outside the region", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        // start in bassin, end outside Occitanie
        {
          start: { ...defaultPosition, arr: occitanie2026Data.areas[0] },
          end: { ...defaultPosition, reg: "84" },
        },
        // start outside Occitanie, end in bassin
        {
          start: { ...defaultPosition, reg: "84" },
          end: { ...defaultPosition, arr: occitanie2026Data.areas[0] },
        },
      ],
    }, { incentive: [150, 150] }));

  it("operator class not eligible", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_class: "A" }],
    }, { incentive: [0] }));

  it("passenger contribution too low", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { passenger_contribution: 0 },
        { passenger_contribution: 49 },
      ],
    }, { incentive: [0, 0] }));

  it("trip outside of campaign dates", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ datetime: new Date("2022-09-02") }],
    }, { incentive: [0] }));
});

describe("Occitanie 2027 - Incentives", () => {
  // De 2 à 20km : 2€ par passager transporté - contribution passager
  it("first slice (2-20)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 2_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },
        { distance: 2_000, passenger_identity_key: uuidV4(), passenger_contribution: 200 },
        { distance: 10_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },
        { distance: 10_000, passenger_identity_key: uuidV4(), passenger_contribution: 50 },
        { distance: 15_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },
        { distance: 15_000, passenger_identity_key: uuidV4(), passenger_contribution: 250 }, // possible ?
      ],
    }, { incentive: [100, 0, 100, 150, 100, 0] }));

  // De 20 à 30km : 0,10 € par trajet par km par passager moins la contribution du passager, plafonné à 2,00 €
  it("second slice (20-30)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        // 20 x 0,10 - 0,50 = 1,50 €
        { distance: 20_000, passenger_identity_key: uuidV4(), passenger_contribution: 50 },

        // 20 x 0,10 - 1,00 = 1,00 €
        { distance: 20_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },

        // 25 * 0,10 - 0,50 = 2,00 €
        { distance: 25_000, passenger_identity_key: uuidV4(), passenger_contribution: 50 },

        // 25 * 0,10 - 1,00 = 1,50 €
        { distance: 25_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },

        // 29 * 0,10 - 0,50 = 2,40 € -> capped at 2,00 €
        { distance: 29_000, passenger_identity_key: uuidV4(), passenger_contribution: 50 },

        // 29 * 0,10 - 1,00 = 1,90 €
        { distance: 29_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },

        // 29 * 0,10 - 2,00 = 0,90 €
        { distance: 29_000, driver_identity_key: uuidV4(), passenger_contribution: 200 }, // switch driver identity to avoid daily limit
      ],
    }, { incentive: [150, 100, 200, 150, 200, 190, 90] }));

  // Pour les trajets au-delà de 30km et jusqu’à 50km sur les bassins listés en annexe 1 :
  // plafonné à 2€ par passager transporté
  it("third slice (30-50) specific areas", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 30_000, start: { ...defaultCarpool.start, arr: occitanie2026Data.areas[0] } },
        { distance: 49_999, start: { ...defaultCarpool.start, arr: occitanie2026Data.areas[0] } },
      ],
    }, { incentive: [200, 200] }));

  it("farther away", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 30_000 },
        { distance: 50_000, start: { ...defaultCarpool.start, arr: occitanie2026Data.areas[0] } },
      ],
    }, { incentive: [0, 0] }));
});

describe("Occitanie 2026 - Driver daily limits", () => {
  it("should limit driver daily trips to 6", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() }, /* exceeds driver limit here */
      ],
    }, { incentive: [150, 150, 150, 150, 150, 150, 0] }));

  it("should reset driver limits on different days", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { datetime: new Date("2026-02-02T10:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T12:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T14:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T16:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T16:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T16:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T18:00:00"), passenger_identity_key: uuidV4() }, // exceeds driver limit here

        // next day
        { datetime: new Date("2026-02-03T10:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-03T12:00:00"), passenger_identity_key: uuidV4() },
      ],
    }, { incentive: [150, 150, 150, 150, 150, 150, 0, 150, 150] }));
});

describe("Occitanie 2026 - Passenger daily limits", () => {
  it("should limit passenger daily trips to 2", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { operator_trip_id: uuidV4(), driver_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), driver_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), driver_identity_key: uuidV4() }, /* exceeds driver limit here */
      ],
    }, { incentive: [150, 150, 0] }));

  it("should reset passenger limits on different days", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { datetime: new Date("2026-02-02T10:00:00"), driver_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T12:00:00"), driver_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T18:00:00"), driver_identity_key: uuidV4() }, // exceeds driver limit here

        // next day
        { datetime: new Date("2026-02-03T10:00:00"), driver_identity_key: uuidV4() },
        { datetime: new Date("2026-02-03T12:00:00"), driver_identity_key: uuidV4() },
      ],
    }, { incentive: [150, 150, 0, 150, 150] }));
});

describe("Occitanie 2026 - Campaign global limit", () => {
  it("should limit campaign total amount to 210 000 €", async () => {
    await process({
      policy: { handler: Handler.id, max_amount: 61_163_00 },
      carpool: [{}, {}, {}],
      meta: [{
        key: "max_amount_restriction.global.campaign.global",
        value: 61_162_00,
      }],
    }, {
      incentive: [100, 0, 0], // 1.00€ left to reach the limit
    });
  });
});

describe("Occitanie 2026 - Booster octobre 2026", () => {
  const booster = new Date("2026-10-05T10:00:00+0200"); // Monday
  const bassin = { ...defaultPosition, arr: occitanie2026Data.areas[0] };

  // read by the APDF export to split trips between normal and booster tables
  it("exposes booster dates in params", () => {
    const dates = new Handler(0).params().booster_dates ?? [];
    assertEquals(dates.length, 31);
    assertEquals(dates[0], "2026-10-01");
    assertEquals(dates[30], "2026-10-31");
  });

  it("period bounds (Europe/Paris)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { datetime: new Date("2026-09-30T23:59:59+0200"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-10-01T00:00:00+0200"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-10-31T23:59:59+0100"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-11-01T00:00:00+0100"), passenger_identity_key: uuidV4() },
      ],
    }, { incentive: [150, 250, 250, 150] }));

  // 2-7 km : 2,50 € ; 7-50 km : 3,50 € par passager, sans déduire la contribution ni plafond à 2 €
  it("slices", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { datetime: booster, distance: 2_000, passenger_identity_key: uuidV4() },
        { datetime: booster, distance: 6_999, passenger_identity_key: uuidV4(), passenger_contribution: 150 },
        { datetime: booster, distance: 7_000, passenger_identity_key: uuidV4() },
        { datetime: booster, distance: 35_000, passenger_identity_key: uuidV4(), passenger_contribution: 200 },
        { datetime: booster, distance: 50_000, passenger_identity_key: uuidV4() },
        { datetime: booster, distance: 10_000, passenger_identity_key: uuidV4(), seats: 2 },
      ],
    }, { incentive: [250, 250, 350, 350, 350, 700] }));

  it("distance limits apply to every zone", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { datetime: booster, distance: 1_999, passenger_identity_key: uuidV4() },
        { datetime: booster, distance: 50_001, passenger_identity_key: uuidV4() },
        { datetime: booster, distance: 50_001, passenger_identity_key: uuidV4(), start: bassin },
        {
          datetime: booster,
          distance: 40_000,
          passenger_identity_key: uuidV4(),
          start: bassin,
          end: { ...defaultPosition, reg: "84" },
        },
      ],
    }, { incentive: [0, 0, 0, 350] }));

  it("passenger contribution between 0,50 € and 2,00 €", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { datetime: booster, passenger_identity_key: uuidV4(), passenger_contribution: 49 },
        { datetime: booster, passenger_identity_key: uuidV4(), passenger_contribution: 201 },
      ],
    }, { incentive: [0, 0] }));

  it("geographic exclusions still apply", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        {
          datetime: booster,
          passenger_identity_key: uuidV4(),
          start: { ...defaultPosition, aom: occitanie2026Data.inner[0] },
          end: { ...defaultPosition, aom: occitanie2026Data.inner[0] },
        },
        { datetime: booster, passenger_identity_key: uuidV4(), end: { ...defaultPosition, reg: "84" } },
      ],
    }, { incentive: [0, 0] }));
});

describe("Occitanie 2026 - Bassins : trajets internes à une AOM locale", () => {
  const rodez = { ...defaultPosition, arr: "12202", aom: "241200187" };

  it("Rodez -> Rodez rejected, normal period and booster", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { start: rodez, end: rodez, passenger_identity_key: uuidV4() },
        { start: rodez, end: rodez, datetime: new Date("2026-10-05T10:00:00+0200"), passenger_identity_key: uuidV4() },
      ],
    }, { incentive: [0, 0] }));

  it("Rodez -> other AOM accepted", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ start: rodez, passenger_identity_key: uuidV4() }],
    }, { incentive: [150] }));
});

describe("Occitanie 2026 - Booster : financement employeur", () => {
  const operatorSiret = "49190454600000";
  const withIncentive = (siret: string, extra: Partial<CarpoolInterface> = {}) => ({
    datetime: new Date("2026-10-05T10:00:00+0200"),
    passenger_identity_key: uuidV4(),
    passenger_contribution: 0,
    operator_siret: operatorSiret,
    operator_incentives: [{ siret, amount: 300 }],
    ...extra,
  });

  it("employer incentive makes a free trip eligible", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [withIncentive("38347481400000")],
    }, { incentive: [250] }));

  it("incentive from Région, operator, AOM, AOM delegate or Agen does not count", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        withIncentive("20005379100000"),
        withIncentive("49190454600000"),
        withIncentive("24120018700000"),
        withIncentive("81317849800000"),
        withIncentive("20003545900000"),
        withIncentive("20005379100000", { operator_incentives: [] }),
      ],
    }, { incentive: [0, 0, 0, 0, 0, 0] }));

  it("one employer line among others is enough", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [withIncentive("20005379100000", {
        operator_incentives: [{ siret: "20005379100000", amount: 100 }, { siret: "38347481400000", amount: 200 }],
      })],
    }, { incentive: [250] }));

  it("outside booster the contribution minimum stays", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [withIncentive("38347481400000", { datetime: new Date("2026-09-15T10:00:00+0200") })],
    }, { incentive: [0] }));
});

describe("Occitanie 2026 - Exception employeur du booster (table)", () => {
  const EMPLOYER = "38347481400000";
  const OPERATOR = "49190454600000";
  const LOCAL_AOM = "24120018700000";
  const DELEGATE = "81317849800000";
  const KEOLIS_TARBES = "39362393900000";
  const REGION = "20005379100000";
  const BOOSTER = "2026-10-15T10:00:00+0200";

  // incentive attendu en centimes : 0 = rejeté
  const cases: Array<{ name: string; datetime: string; contribution: number; funders: string[]; expected: number }> = [
    {
      name: "30/09 23:59, 0 €, employeur",
      datetime: "2026-09-30T23:59:00+0200",
      contribution: 0,
      funders: [EMPLOYER],
      expected: 0,
    },
    {
      name: "01/10 00:00, 0 €, employeur",
      datetime: "2026-10-01T00:00:00+0200",
      contribution: 0,
      funders: [EMPLOYER],
      expected: 250,
    },
    { name: "15/10, 0,49 €, employeur", datetime: BOOSTER, contribution: 49, funders: [EMPLOYER], expected: 250 },
    { name: "15/10, 0 €, aucun financeur", datetime: BOOSTER, contribution: 0, funders: [], expected: 0 },
    { name: "15/10, 0 €, opérateur seul", datetime: BOOSTER, contribution: 0, funders: [OPERATOR], expected: 0 },
    { name: "15/10, 0 €, AOM locale seule", datetime: BOOSTER, contribution: 0, funders: [LOCAL_AOM], expected: 0 },
    { name: "15/10, 0 €, délégataire seul", datetime: BOOSTER, contribution: 0, funders: [DELEGATE], expected: 0 },
    {
      name: "15/10, 0 €, Keolis Tarbes seul",
      datetime: BOOSTER,
      contribution: 0,
      funders: [KEOLIS_TARBES],
      expected: 0,
    },
    { name: "15/10, 0 €, Région seule", datetime: BOOSTER, contribution: 0, funders: [REGION], expected: 0 },
    {
      name: "15/10, 0 €, employeur + AOM locale",
      datetime: BOOSTER,
      contribution: 0,
      funders: [EMPLOYER, LOCAL_AOM],
      expected: 250,
    },
    {
      name: "31/10 23:59, 0 €, employeur",
      datetime: "2026-10-31T23:59:00+0100",
      contribution: 0,
      funders: [EMPLOYER],
      expected: 250,
    },
    {
      name: "01/11 00:00, 0 €, employeur",
      datetime: "2026-11-01T00:00:00+0100",
      contribution: 0,
      funders: [EMPLOYER],
      expected: 0,
    },
    { name: "15/10, 2,01 €, employeur", datetime: BOOSTER, contribution: 201, funders: [EMPLOYER], expected: 0 },
  ];

  for (const c of cases) {
    it(c.name, async () =>
      await process({
        policy: { handler: Handler.id },
        carpool: [{
          datetime: new Date(c.datetime),
          passenger_contribution: c.contribution,
          operator_siret: OPERATOR,
          operator_incentives: c.funders.map((siret) => ({ siret, amount: 300 })),
        }],
      }, { incentive: [c.expected] }));
  }
});

describe("Occitanie 2026 - Matrice des trajets entre AOM (table)", () => {
  const place = (com: string, aom: string, reg = "76") => ({ ...defaultPosition, com, arr: com, aom, reg });
  const T = {
    R: place("46240", "200053791"), // AOM Région, hors bassin
    R2: place("46042", "200053791"), // Cahors
    Narbonne: place("11262", "241100593"),
    Beziers: place("34032", "243400769"),
    Montpellier: place("34172", "243400017"),
    Mauguio: place("34154", "243400470"), // Pays de l'Or
    Sete: place("34301", "200066355"),
    Rodez: place("12202", "241200187"), // bassin
    Ales: place("30007", "200003325"), // bassin
    Mende: place("48095", "244800470"), // bassin, AOM CC du Gévaudan (annexe 2)
    LesAngles: place("30011", "248400251"), // type A : Grand Avignon
    Barcelonne: place("32027", "200000000"), // type A : AOM Nouvelle-Aquitaine (SIREN fictif)
    Avignon: place("84007", "248400251", "93"),
    Agen: place("47001", "200035459", "75"),
  };
  type Place = keyof typeof T;

  const JUL = "2026-07-15T10:00:00+0200"; // aucune AOM réintégrée
  const AUG = "2026-08-15T10:00:00+0200"; // Montpellier réintégrée
  const SEP = "2026-09-10T10:00:00+0200"; // Montpellier et Pays de l'Or réintégrés, Sète exclue
  const OCT = "2026-10-15T10:00:00+0200"; // booster

  type Row = { date: string; a: Place; b: Place; ok: boolean; distance?: number };

  // Jouées dans les deux sens ; hors booster : 5 km = 1,50 €, bassin 40 km = 2,00 €
  const rows: Row[] = [
    { date: SEP, a: "R", b: "R", ok: true },
    { date: SEP, a: "R", b: "R2", ok: true },
    { date: SEP, a: "R", b: "Narbonne", ok: true },
    { date: SEP, a: "R", b: "Montpellier", ok: true }, // Lr
    { date: JUL, a: "R", b: "Montpellier", ok: true }, // Lx
    { date: SEP, a: "R", b: "Sete", ok: true },
    { date: SEP, a: "Narbonne", b: "Narbonne", ok: false },
    { date: SEP, a: "Sete", b: "Sete", ok: false },
    { date: JUL, a: "Montpellier", b: "Montpellier", ok: false },
    { date: SEP, a: "Narbonne", b: "Beziers", ok: true },
    { date: SEP, a: "Montpellier", b: "Montpellier", ok: true },
    { date: SEP, a: "Montpellier", b: "Mauguio", ok: true }, // deux AOM réintégrées
    { date: AUG, a: "Montpellier", b: "Sete", ok: true }, // Lr, Lx
    { date: SEP, a: "Montpellier", b: "Narbonne", ok: true },
    { date: SEP, a: "R", b: "Avignon", ok: false },
    { date: SEP, a: "Narbonne", b: "Agen", ok: false },
    { date: SEP, a: "Montpellier", b: "Avignon", ok: false },
    { date: SEP, a: "Avignon", b: "Agen", ok: false },
    { date: SEP, a: "Avignon", b: "Avignon", ok: false },
    { date: SEP, a: "R", b: "LesAngles", ok: false },
    { date: SEP, a: "Narbonne", b: "Barcelonne", ok: false },
    { date: SEP, a: "Montpellier", b: "LesAngles", ok: false },
    { date: SEP, a: "LesAngles", b: "LesAngles", ok: false },
    { date: SEP, a: "LesAngles", b: "Barcelonne", ok: false },
    // bassins, 40 km
    { date: SEP, a: "Rodez", b: "R", ok: true, distance: 40_000 },
    { date: SEP, a: "Rodez", b: "Narbonne", ok: true, distance: 40_000 },
    { date: SEP, a: "Rodez", b: "Montpellier", ok: true, distance: 40_000 },
    { date: SEP, a: "Rodez", b: "Rodez", ok: false, distance: 40_000 },
    { date: SEP, a: "Ales", b: "Ales", ok: false, distance: 40_000 },
    { date: SEP, a: "Mende", b: "Mende", ok: false, distance: 40_000 },
    { date: SEP, a: "Rodez", b: "LesAngles", ok: true, distance: 40_000 },
    { date: SEP, a: "Rodez", b: "Avignon", ok: true, distance: 40_000 },
    { date: SEP, a: "Agen", b: "Mende", ok: true, distance: 40_000 },
    // hors bassin : tranche longue refusée
    { date: SEP, a: "R", b: "R2", ok: false, distance: 40_000 },
    { date: SEP, a: "Narbonne", b: "Beziers", ok: false, distance: 40_000 },
  ];

  // Régime booster (2,50 € fixe, 3,50 € au-delà de 7 km) : lignes « même AOM » et « deux AOM différentes »
  const boosterRows: Row[] = [
    { date: OCT, a: "R", b: "R", ok: true },
    { date: OCT, a: "Narbonne", b: "Narbonne", ok: false },
    { date: OCT, a: "Narbonne", b: "Beziers", ok: true },
    { date: OCT, a: "Montpellier", b: "Montpellier", ok: true },
    { date: OCT, a: "Sete", b: "Sete", ok: true },
    { date: OCT, a: "Mauguio", b: "Mauguio", ok: true },
    { date: OCT, a: "Montpellier", b: "Mauguio", ok: true },
    { date: OCT, a: "Montpellier", b: "Narbonne", ok: true },
    { date: OCT, a: "R", b: "Avignon", ok: false },
    { date: OCT, a: "Rodez", b: "Rodez", ok: false, distance: 40_000 },
    { date: OCT, a: "Rodez", b: "Avignon", ok: true, distance: 40_000 },
    { date: OCT, a: "R", b: "R2", ok: true, distance: 40_000 }, // booster : 50 km sur toutes les zones
  ];

  const label = ({ date, a, b, ok, distance }: Row) =>
    `${date.slice(0, 10)} ${a} <-> ${b}${distance ? ` ${distance / 1000} km` : ""} : ${ok ? "incité" : "rejeté"}`;

  const play = (list: Row[], prefix: string, amount: (r: Row) => number) => {
    for (const row of list) {
      for (const [from, to] of [[row.a, row.b], [row.b, row.a]] as Array<[Place, Place]>) {
        it(`${prefix}${label(row)} (${from} -> ${to})`, async () =>
          await process({
            policy: { handler: Handler.id },
            carpool: [{ datetime: new Date(row.date), distance: row.distance ?? 5_000, start: T[from], end: T[to] }],
          }, { incentive: [row.ok ? amount(row) : 0] }));
      }
    }
  };

  play(rows, "", (r) => (r.distance ? 200 : 150));
  play(boosterRows, "booster ", (r) => (r.distance ? 350 : 250));
});

describe("Occitanie 2026 - Bascules de fin de campagne des AOM locales", () => {
  const place = (com: string, aom: string) => ({ ...defaultPosition, com, arr: com, aom });
  const montpellier = place("34172", "243400017");
  const paysDeLOr = place("34154", "243400470");
  const sete = place("34301", "200066355");
  const narbonne = place("11262", "241100593");
  const internal = (p: typeof montpellier) => ({
    start: p,
    end: p,
    passenger_contribution: 100,
    distance: 10_000,
    passenger_identity_key: uuidV4(),
  });

  const switchovers = [
    {
      name: "Montpellier",
      p: montpellier,
      lastRejected: "2026-07-31T23:59:59+0200",
      firstOk: "2026-08-01T00:00:00+0200",
    },
    {
      name: "Pays de l'Or",
      p: paysDeLOr,
      lastRejected: "2026-08-31T23:59:59+0200",
      firstOk: "2026-09-01T00:00:00+0200",
    },
    { name: "Sète", p: sete, lastRejected: "2026-09-28T23:59:59+0200", firstOk: "2026-09-29T00:00:00+0200" },
  ];

  for (const { name, p, lastRejected, firstOk } of switchovers) {
    it(`${name} : bascule à la seconde (Paris)`, async () =>
      await process({
        policy: { handler: Handler.id },
        carpool: [
          { ...internal(p), datetime: new Date(lastRejected) },
          { ...internal(p), datetime: new Date(firstOk) },
        ],
      }, { incentive: [0, 100] }));
  }

  it("2026-08-15 : seule Montpellier réintégrée", async () => {
    const datetime = new Date("2026-08-15T10:00:00+0200");
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { ...internal(montpellier), datetime },
        { ...internal(paysDeLOr), datetime },
        { ...internal(sete), datetime },
        { ...internal(montpellier), end: sete, datetime },
      ],
    }, { incentive: [100, 0, 0, 100] });
  });

  it("2026-09-15 : Pays de l'Or réintégrée, Sète exclue", async () => {
    const datetime = new Date("2026-09-15T10:00:00+0200");
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { ...internal(paysDeLOr), datetime },
        { ...internal(sete), datetime },
      ],
    }, { incentive: [100, 0] });
  });

  it("2026-10-15 booster : les trois réintégrées au tarif booster", async () => {
    const datetime = new Date("2026-10-15T10:00:00+0200");
    await process({
      policy: { handler: Handler.id },
      carpool: [montpellier, paysDeLOr, sete].map((p) => ({ ...internal(p), datetime })),
    }, { incentive: [350, 350, 350] });
  });

  it("Narbonne -> Narbonne (sans date de fin) reste rejetée", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        "2026-07-31T23:59:59+0200",
        "2026-08-15T10:00:00+0200",
        "2026-09-15T10:00:00+0200",
        "2026-09-29T00:00:00+0200",
        "2026-10-15T10:00:00+0200",
        "2026-11-15T10:00:00+0100",
      ].map((d) => ({ ...internal(narbonne), datetime: new Date(d) })),
    }, { incentive: [0, 0, 0, 0, 0, 0] }));

  it("annexe 2 complète conservée dans inner et notEmployerSirens", () => {
    for (const siren of ["243400017", "243400470", "200066355"]) {
      assertEquals(occitanie2026Data.inner.includes(siren as TerritoryCodeEnum.Mobility), true);
      assertEquals(occitanie2026Data.notEmployerSirens.has(siren), true);
    }
    assertEquals(occitanie2026Data.notEmployerSirens.has("393623939"), true);
  });
});
