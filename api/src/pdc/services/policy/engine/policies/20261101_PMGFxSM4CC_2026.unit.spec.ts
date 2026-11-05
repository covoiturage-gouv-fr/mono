import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { PMGFxSM4CC2026 as Handler } from "./20261101_PMGFxSM4CC_2026.ts";

const PMGF_SIRET = "20007537200017";

const defaultPosition = {
  arr: "74206",
  com: "74206",
  aom: "200067551",
  epci: "200067551",
  dep: "74",
  reg: "84",
  country: "XXXXX",
};

const defaultLat = 46.313355215729146;
const defaultLon = 6.487631441991693;

const defaultCarpool = {
  _id: 1,
  operator_trip_id: uuidV4(),
  passenger_identity_key: uuidV4(),
  driver_identity_key: uuidV4(),
  operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2026-11-15"),
  seats: 1,
  duration: 600,
  distance: 10_000,
  operator_journey_id: uuidV4(),
  operator_id: 1,
  driver_revenue: 20,
  passenger_contribution: 20,
  start: { ...defaultPosition },
  end: { ...defaultPosition },
  start_lat: defaultLat,
  start_lon: defaultLon,
  end_lat: defaultLat,
  end_lon: defaultLon,
  operator_incentives: [{ siret: PMGF_SIRET, amount: 150 }],
};

const process = makeProcessHelper(defaultCarpool);

const outsidePosition = {
  arr: "69123",
  com: "69123",
  aom: "200046977",
  epci: "200046977",
  dep: "69",
  reg: "84",
  country: "XXXXX",
};

const quatreRivieres74 = { ...defaultPosition, arr: "74056", com: "74056", aom: "247400666", epci: "247400666" };
const quatreRivieresEnBray = { ...outsidePosition, arr: "76001", com: "76001", aom: "200069730", epci: "200069730" };

it("should work with exclusions", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { operator_uuid: "not in list" },
        { distance: 4_000 },
        { distance: 81_000 },
        { operator_class: "A" },
        { start: { ...outsidePosition }, end: { ...outsidePosition } },
        { start: { ...quatreRivieresEnBray }, end: { ...quatreRivieresEnBray } },
      ],
      meta: [],
    },
    { incentive: [0, 0, 0, 0, 0, 0], meta: [] },
  ));

it("uses the incentive declared by the operator for the PMGF SIREN", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { operator_incentives: [{ siret: PMGF_SIRET, amount: 150 }] },
        // Autre SIRET du PMGF (même SIREN)
        { operator_incentives: [{ siret: "20007537200099", amount: 275 }] },
        // Financeur voisin seul : rien pour le PMGF
        { operator_incentives: [{ siret: "20006799900090", amount: 300 }] },
        // Deux financeurs : seule la part PMGF compte
        {
          operator_incentives: [
            { siret: "20006799900090", amount: 100 },
            { siret: PMGF_SIRET, amount: 200 },
          ],
        },
        // Aucune incitation déclarée
        { operator_incentives: [] },
        { operator_incentives: undefined },
        // Une seule extrémité sur le territoire
        { start: { ...outsidePosition }, operator_incentives: [{ siret: PMGF_SIRET, amount: 100 }] },
        // CC des Quatre Rivières (74)
        {
          start: { ...quatreRivieres74 },
          end: { ...quatreRivieres74 },
          operator_incentives: [{ siret: PMGF_SIRET, amount: 150 }],
        },
      ],
    },
    { incentive: [150, 275, 0, 200, 0, 0, 100, 150] },
  ));

it("matches member EPCI whose AOM became the PMGF in 2026", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        // CA Annemasse-Les Voirons
        {
          start: { ...outsidePosition },
          end: { ...defaultPosition, aom: "200075372", epci: "200011773" },
        },
        // CC du Genevois
        {
          start: { ...defaultPosition, aom: "200075372", epci: "247400690" },
          end: { ...outsidePosition },
        },
      ],
    },
    { incentive: [150, 150] },
  ));

it("should enforce driver monthly amount limit (50 EUR)", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { driver_identity_key: "one" },
        { driver_identity_key: "one" },
      ],
      meta: [
        {
          key: "max_amount_restriction.0-one.month.10-2026",
          value: 49_00,
        },
      ],
    },
    {
      incentive: [100, 0],
      meta: [
        {
          key: "max_amount_restriction.0-one.month.10-2026",
          value: 50_00,
        },
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 100,
        },
      ],
    },
  ));

it("exposes the declared incentives SIREN for the APDF", () => {
  const params = new Handler(25_000_00).params();
  assertEquals(params.extras, { declared_incentives: { siren: "200075372" } });
  assertEquals(params.limits, { glob: 25_000_00 });
});
