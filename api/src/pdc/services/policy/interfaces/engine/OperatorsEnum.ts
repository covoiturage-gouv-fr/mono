// Liste des opérateurs identifiés par leur UUID
// pour le matching avec les campagnes.
//
// La requête ci-dessous exporte les opérateurs actifs et leur UUID
// au format enum TypeScript.
// ```sql
// SELECT
//   regexp_replace(UPPER(name), '[^A-Z]', '_', 'g') || ' = ' || '''' || uuid || '''' || ',' as OperatorsEnum
// FROM operator.operators
// WHERE deleted_at IS NULL
// ORDER BY 1;
// ```
//
export enum OperatorsEnum {
  ATCHOUM = "25a8774f-8708-4cf7-8527-446106b64a35",
  BLABLACAR = "51dba52f-913c-47fe-a976-25a799374f7f",
  BLABLACAR_DAILY = "0b361f5b-4651-45f1-8f59-5952d5e745fd",
  CILIGO = "668039e6-01e5-40ac-810e-4bee6acabfb1",
  CITYGO = "0074c9d2-943a-408b-bce2-d9820526e866",
  COCCINELLE = "41a087ae-ba23-4ea3-9046-4abe5bb3746a",
  CONVOITILE = "67548415-486b-4299-a3e8-d2952c730845",
  COVOIT_IDFM = "0b3e40cd-5a7d-401b-825b-f49d074f7290",
  COVOITURAGE_ENTREPRISE = "9d18e190-c845-4f4b-94a2-208052176de7",
  COVOITURAGE_OISE_FR = "d35755fe-8f44-4db4-9387-53e8d8f43104",
  COVOIZZ = "83d68f80-ddf0-4cbc-a5f4-bd6cbbb0a295",
  ECHANGERSAMAISON = "93920d91-2458-4f80-8fd3-a0b1de64b922",
  ECOV = "00e64a84-7b1d-40c9-8b09-94b2f0e3b55a",
  FRANCECOVOIT = "1dc68aa2-10a0-4ccb-9277-c5d5c3f83401",
  ILLICOV = "1291d134-2142-4ad0-921b-7413abc8898e",
  INSTANT_SYSTEM = "849dc1a8-5569-4e96-877a-34fcd1323982",
  KAROS = "10c5fbcc-08b5-490e-a2cc-8ee2890f0a80",
  KEEPMOVE = "31fcbd66-e27d-484b-8afb-d21bd5b01d8c",
  KLAXIT = "189e8a6f-19fc-4a8c-b8fc-c421ff54b4c5",
  LA_ROUE_VERTE = "87ee1541-1ffa-48f5-9824-ac9cb0cff80d",
  LE_PTIT_ROGER = "3d590de4-824e-4933-9234-b94bc6097d9e",
  M_COVOITMODALIS_FR = "59440576-6651-49bb-b8b8-211974aaf3a3",
  MOBICOOP = "5ae9a895-87aa-42e5-a984-2ae8065d8bb0",
  MOBIGO = "35449f29-43a8-4df5-9cd0-7d1798d869fa",
  MOBIL_AUDE = "c7d32c81-4d3e-4e80-bfa5-a9afc79e95f5",
  MOOVANCE = "c3ee7c45-aea0-413d-b47d-aa6e41bb003b",
  MOV_ICI = "8d352aea-09ae-49b2-992d-71bcf8108efa",
  NOULA = "0ec26589-f3ee-41a0-a315-8084bbc9c16e",
  OUESTGO = "22a8b5ab-9083-43eb-92d8-528080e4b0cb",
  OXYCAR = "80a6ec0b-4a68-4a6c-bdea-8a808d3ee205",
  PASS_PASS_COVOITURAGE = "a86cfe29-dc0e-400e-aeab-d68670a70aef",
  PICHOLINES = "259de1a1-945b-4dfe-88b4-19c7ade75301",
  RIDYGO = "7b5fa2b3-e6ed-452e-bfe2-32bc0f0fff53",
  ROULEZMALIN = "86ca3013-125e-4a3f-b025-a1f83e3bb7a2",
  RUNDRIVE = "5d63065c-00b3-463c-b3be-83e9ad011ebd",
  SAS_DEPOZE = "0a9e5362-68f6-4c8c-9771-83c90f4638de",
  SIMONES = "95cbccb7-55f9-4398-a0cf-476d37e71446",
  SNCF_MOBILITY = "3416a47d-c656-4fa7-b12f-603fe9296f8d",
  STADIUMGO = "db0a0617-c73a-4caa-80c3-efb0b23781e1",
  STOPNGO_CAR = "8e9dc5ae-967c-40cf-9268-7007170c4740",
  YACKA = "7836fa7c-3ce5-42d6-a637-e6cf045d3a2a",
  YNSTANT = "d0f41313-3424-4b89-8045-076e88177e7d",
}
