import { assertEquals, assertRejects } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { ConflictException, NotFoundException } from "@/ilos/common/index.ts";
import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import type { CreateCampaignCondition } from "@/pdc/services/dashboard/dto/CampaignConditions.ts";
import { CampaignConditionsRepository } from "./CampaignConditionsRepository.ts";

describe("CampaignConditionsRepository", () => {
  let db: DenoDbContext;
  let repository: CampaignConditionsRepository;
  let op1: number;
  let op2: number;
  const { before, after } = makeDenoDbBeforeAfter();

  const campaign = (overrides: Partial<CreateCampaignCondition> = {}): CreateCampaignCondition => ({
    territory_type: "custom",
    territory_code: "36709",
    territory_label: "PETR Alsace du Nord",
    operator_ids: [op1],
    premiere_campagne: false,
    date_debut: "2026-01-01",
    date_fin: "2026-12-31",
    budget_incitations: 5000000,
    conducteur_montant_min_par_passager: 150,
    conducteur_montant_max_par_passager: 300,
    conducteur_montant_max_par_mois: null,
    conducteur_trajets_max_par_mois: null,
    passager_gratuite: true,
    passager_eligible_gratuite: "tous",
    passager_reduction_ticket: false,
    passager_eligibilite_reduction: null,
    passager_montant_ticket: null,
    passager_trajets_max_par_mois: null,
    zone_sens_des_trajets: "OU",
    zone_exclusion: false,
    zone_exclusion_liste: null,
    autre_exclusion: null,
    trajet_longueur_min: 5,
    trajet_longueur_max: 80,
    trajet_classe_preuve: ["B", "C"],
    lien_page_collectivite: null,
    autres_informations: null,
    ...overrides,
  });

  beforeAll(async () => {
    db = await before();
    repository = new CampaignConditionsRepository(db.connection);
    // Le jeu de données de test insère des opérateurs avec des _id explicites : on recale la séquence.
    await db.connection.query(sql`
      SELECT setval(pg_get_serial_sequence('operator.operators', '_id'), (SELECT max(_id) FROM operator.operators))
    `);
    const ops = await db.connection.query<{ _id: number }>(sql`
      INSERT INTO operator.operators (name, siret)
      VALUES ('Opérateur 1', '11111111100011'), ('Opérateur 2', '22222222200022')
      RETURNING _id
    `);
    [op1, op2] = ops.map((o) => o._id);
  });

  afterAll(async () => {
    await after(db);
  });

  it("crée une campagne en brouillon et la relit avec ses dates ISO", async () => {
    const { id } = await repository.create(campaign(), { operator_id: op1, territory_id: null });
    const row = await repository.find(id);
    assertEquals(row?.status, "draft");
    assertEquals(row?.created_by_operator_id, op1);
    assertEquals(row?.date_debut, "2026-01-01");
    assertEquals(row?.date_fin, "2026-12-31");
    assertEquals(row?.operator_ids, [op1]);
    assertEquals(row?.trajet_classe_preuve, ["B", "C"]);
  });

  it("refuse un doublon territoire + date de début", async () => {
    await assertRejects(
      () => repository.create(campaign({ operator_ids: [op2] }), { operator_id: op2, territory_id: null }),
      ConflictException,
    );
  });

  it("filtre par opérateur listé", async () => {
    await repository.create(
      campaign({ territory_code: "200067999", territory_type: "aom", operator_ids: [op1, op2] }),
      { operator_id: null, territory_id: null },
    );
    assertEquals((await repository.list({ operator_id: op1 })).length, 2);
    assertEquals((await repository.list({ operator_id: op2 })).length, 1);
    assertEquals((await repository.list({})).length, 2);
  });

  it("valide puis repasse en brouillon à la modification", async () => {
    const [row] = await repository.list({ operator_id: op2 });
    await repository.validate(row.id, null);
    assertEquals((await repository.find(row.id))?.status, "validated");

    await repository.update({ ...campaign({ territory_code: "200067999", territory_type: "aom" }), id: row.id });
    const updated = await repository.find(row.id);
    assertEquals(updated?.status, "draft");
    assertEquals(updated?.validated_at, null);
    assertEquals(updated?.operator_ids, [op1]);
  });

  it("supprime logiquement et libère le couple territoire + date", async () => {
    const [row] = await repository.list({ operator_id: op1 });
    await repository.delete(row.id);
    assertEquals(await repository.find(row.id), null);
    await assertRejects(() => repository.delete(row.id), NotFoundException);
    await repository.create(
      { ...campaign(), territory_code: row.territory_code, territory_type: row.territory_type },
      { operator_id: op1, territory_id: null },
    );
  });

  describe("territoires", () => {
    let aomGroup: number;
    let customGroup: number;

    beforeAll(async () => {
      const groups = await db.connection.query<{ _id: number }>(sql`
        INSERT INTO territory.territory_group (name) VALUES ('AOM test'), ('Custom test') RETURNING _id
      `);
      [aomGroup, customGroup] = groups.map((g) => g._id);
      await db.connection.query(sql`
        INSERT INTO territory.territory_group_selector (territory_group_id, selector_type, selector_value)
        VALUES (${aomGroup}, 'aom', '200046977'), (${customGroup}, 'epci', '200067999'), (${customGroup}, 'epci', '200069730')
      `);
      await db.connection.query(sql`
        INSERT INTO territory.territory_perimeters (territory_id, version, arr, valid_from)
        VALUES (${customGroup}, 1, ARRAY['69123']::varchar[], '2026-01-01')
      `);
    });

    it("résout un sélecteur unique et un périmètre versionné vers les codes du datalake", async () => {
      assertEquals(await repository.resolveTerritory(aomGroup), {
        territory_type: "aom",
        territory_code: "200046977",
        territory_label: "AOM test",
      });
      assertEquals(await repository.resolveTerritory(customGroup), {
        territory_type: "custom",
        territory_code: String(customGroup),
        territory_label: "Custom test",
      });
      assertEquals(await repository.resolveTerritory(999999), null);
    });

    it("liste les campagnes saisies par le territoire ou portant sur lui", async () => {
      const resolved = await repository.resolveTerritory(aomGroup);
      await repository.create(
        campaign({ ...resolved!, date_debut: "2026-03-01", operator_ids: [op1, op2] }),
        { operator_id: null, territory_id: aomGroup },
      );
      await repository.create(
        campaign({ ...resolved!, date_debut: "2026-06-01" }),
        { operator_id: op1, territory_id: null },
      );
      const rows = await repository.list({ territory: { id: aomGroup, resolved } });
      assertEquals(rows.length, 2);
      assertEquals(rows.filter((r) => r.created_by_territory_id === aomGroup).length, 1);
      assertEquals((await repository.list({ territory: { id: customGroup, resolved: null } })).length, 0);
    });
  });
});
