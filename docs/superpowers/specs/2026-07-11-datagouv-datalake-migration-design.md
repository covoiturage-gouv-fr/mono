# Publication data.gouv.fr : sqlmesh/API → datalake

**Date :** 2026-07-11
**Statut :** Design — approuvé pour planification
**Périmètre :** La publication mensuelle open-data sur data.gouv.fr uniquement. C'est le
volet explicitement reporté par le spec de l'export à la demande
(`2026-07-07-on-demand-export-datalake-migration-design.md`, risque #8 : « ne pas
supprimer `export_opendata_list` dans cette migration »). L'export à la demande
(operator/territory) est déjà migré et hors périmètre.

## Problème

Aujourd'hui l'API produit elle-même la publication open-data mensuelle : la commande CLI
`export:datagouv` (`api/src/pdc/services/export/commands/DataGouvCommand.ts`) découpe la
plage par mois, génère un CSV en lisant la vue **sqlmesh** `refined_zone.export_opendata_list`
(via `datagouvListQuery.ts`), calcule des statistiques via `trusted_zone.insee_counters`
(via `datagouvStatsQuery.ts`), construit une description de jeu de données, puis publie la
resource sur data.gouv.fr (`DataGouvAPIProvider` + `DataGouvMetadataProvider`).

Ces deux modèles sqlmesh déménagent dans le datalake (base `datalake_production`, séparée),
inaccessible à l'API. La production du fichier doit donc passer côté datalake.

**Différence clé avec l'export à la demande.** L'export à la demande a gardé un *control
plane* parce que l'API **possède la file** `export.exports` (déclenchée par un utilisateur,
avec état). La publication data.gouv est **mensuelle, sans utilisateur, sans état possédé
par l'API** : l'API n'héberge la commande que par accident historique. Il n'y a donc **rien
à posséder côté API** → on déplace **tout le job** dans le datalake.

## Principes / contraintes

- **L'API sort totalement du circuit data.gouv.** Pas de control plane, pas d'état côté API.
- **Le contrat CSV ne doit pas dériver.** Mêmes colonnes, ordre, en-têtes, délimiteur,
  **sans compression** (`.csv` nu), et format de nom de fichier — identiques au dataset
  publié aujourd'hui. Validation **contre les resources réellement en ligne** sur data.gouv.fr.
- **Le k-anonymat est préservé.** On retire les trajets dont un code INSEE (départ ou
  arrivée) apparaît moins de `min_occurrences` (= 6) fois dans le mois.
- **On ne matérialise pas `export_opendata`.** Le modèle reste une **vue** (économie de
  place en base). Le job mensuel a le droit d'être lent.
- **Vérification TLS et secrets** : les credentials data.gouv.fr migrent vers l'env/secret
  du datalake, gérés par l'ops.

## Architecture

Un **job Python mensuel dans le datalake**, sur le modèle de `export.py` / `export_worker.py` :

- `datalake/pipelines/cmd/datagouv.py` (Typer), recette `just datagouv`, **CronJob k8s
  mensuel** (repo ops).

### Flux (cible)

```
CronJob datalake (mensuel, k8s, repo ops) :
  1. Déterminer le mois cible (défaut : mois précédent ; option --start/--end pour backfill)
  2. Garde de complétude : les agrégats territory_month_arr_{from,to} du mois sont matérialisés
  3. COPY (SELECT … FROM zone_exposed.export_opendata
           WHERE start_date_filter dans le mois
             AND start_insee_count >= :min_occ AND end_insee_count >= :min_occ)
        TO STDOUT (FORMAT CSV, HEADER)  → fichier CSV local (NON compressé)
  4. Stats (requête allégée, cf. §Données) : count_total/exposed/removed/…
  5. Garde « dataset vide » : abandon si count_exposed < 1
  6. Description du jeu de données (texte FR à partir des stats)
  7. Publication data.gouv.fr : get dataset → upload nouvelle resource → set metadata
  8. Rapport JSON → bucket datalake, clé datagouv/logs/<AAAA-MM>.json
```

Pas de file, pas de reprise pilotée par l'API : le CronJob est idempotent et rejouable par
mois (chaque run publie une nouvelle resource, comportement actuel préservé).

## Données & k-anonymat (le point central)

- **Source CSV** : `zone_exposed.export_opendata` (déjà existant). Il **embarque déjà** les
  compteurs d'occurrence INSEE par mois — `start_insee_count` / `end_insee_count`, dérivés
  des agrégats matérialisés `territory_month_arr_from` / `territory_month_arr_to`, avec le
  commentaire explicite *« for filtering by API »*.
- **Conséquence majeure** : **aucun modèle `insee_counters` à porter**. Le rôle de l'ancien
  `trusted_zone.insee_counters` (fournir les compteurs) est absorbé par `export_opendata`.
- **Filtre k-anon dans le job** : `WHERE start_insee_count >= :min_occ AND end_insee_count
  >= :min_occ`. Le modèle reste **non filtré** exprès, pour que les stats puissent compter
  les trajets **exposés ET retirés**.
- **Stats — attention aux performances.** On ne calcule **pas** les compteurs à travers la
  vue `export_opendata` : elle fait une **jointure FDW sur les positions GPS** (nécessaire
  au CSV, inutile pour compter). Les stats (`count_total`, `count_exposed`,
  `count_removed`, `count_removed_start`, `count_removed_end`, `count_removed_both`) se
  calculent par une **requête allégée** — `trusted.carpools` × `territory_month_arr_{from,to}`
  (tous deux matérialisés), `COUNT(*) FILTER (… >= :min_occ)`, **sans positions ni
  périmètres**, bornée au mois cible (pushdown d'index). Le CSV, lui, paie la vue complète
  **une seule fois** par mois.

## Composants

### 1. Extraction CSV
`psycopg` `COPY (SELECT … WHERE k-anon … AND mois) TO STDOUT (FORMAT CSV, HEADER,
DELIMITER '<contrat>')` en streaming vers un fichier local, **sans compression**. L'ordre
des colonnes du SELECT **définit le CSV** → il doit reproduire exactement l'ordre du contrat
(cf. tâche bloquante ci-dessous).

### 2. Client data.gouv.fr (décision à confirmer par spike)
La lib officielle **[`datagouv/datagouv_client`](https://github.com/datagouv/datagouv_client)**
est maintenue par l'équipe data.gouv.fr (v0.5.0 en juin 2026, Python ≥ 3.10) et couvre le
besoin : `create_static_resource(file_to_upload=…, payload=…)`, update de resource/métadonnée,
get dataset.

**Décision** : la retenir, **gatée par un spike court** validant, contre un dataset de test :
(a) auth par clé API, (b) upload d'une **nouvelle** resource par mois, (c) mise à jour de la
description, (d) gestion d'erreurs exploitable. **Fallback** : portage de `DataGouvAPIProvider`
(≈ 140 lignes, `requests`, déjà dep) si le spike révèle un manque. Réserves de la lib :
pré-1.0, faible adoption (14★).

La **description FR** du jeu de données (à partir des stats) est portée de
`DataGouvMetadataProvider`.

### 3. Config / secrets
`APP_DATAGOUV_*` (`enabled`, `upload`, `notify`, `contact`, `key`, `url`, `dataset`) migrent
vers l'env/secret du datalake. Câblage secret côté ops.

### 4. Rapports d'export (nouveau)
Chaque run écrit un **rapport JSON** dans le bucket S3 du datalake sous **`datagouv/logs/`**
(p. ex. `datagouv/logs/<AAAA-MM>.json`) : mois, horodatages début/fin, stats détaillées
(total / exposés / retirés + ventilation début / arrivée / les-deux), nom du fichier,
`min_occurrences` appliqué, id + URL de la resource data.gouv publiée, statut succès/échec +
message d'erreur. Donne un historique auditable, remplace `LogService`/`NotificationService`
de l'API pour ce flux.

## Contrat CSV & publication (tâche bloquante)

Le CSV publié a un contrat que des consommateurs externes parsent. Reproduire **à
l'identique** : ensemble et **ordre** des colonnes (cf. `config/datagouv.ts` `fields`),
noms d'en-tête, délimiteur, absence de compression, format du nom de fichier
(`NameService.datagouv`), et **une nouvelle resource par mois** sur le dataset.

**Validation** : diff **à trois voies** sur un mois settled — (a) resource **réellement
publiée en ligne** sur le dataset data.gouv.fr (récupérée via l'API), (b) sortie actuelle de
l'API, (c) sortie du nouveau job datalake. Toute dérive silencieuse casse les parseurs aval.

## Garde-fous & fraîcheur

- **Refus de dataset vide** : abandon bruyant si `count_exposed < 1` (port de
  `assertDatagouvNotEmpty`). Protège contre l'upload d'un fichier vide si l'amont n'est pas
  prêt.
- **Complétude du mois** : on publie le **mois précédent** (défaut). Les agrégats
  `territory_month_arr_{from,to}` du mois doivent être matérialisés — garde explicite avant
  extraction (analogue à la fraîcheur trusted de l'export à la demande).
- **Rejouable** : plage `--start/--end` conservée (backfill d'historique) ; nouvelle resource
  par run.

## Décommission (finalise le report de l'export à la demande)

Une fois la parité validée et le CronJob en prod :

- **API** : supprimer `DataGouvCommand`, `datagouvListQuery`, `datagouvStatsQuery`, les
  providers `providers/datagouv/*`, la config `config/datagouv.ts`, et le câblage associé.
- **sqlmesh** : `DROP` de `refined_zone.export_opendata_list` **et** de
  `trusted_zone.insee_counters` (plus aucun lecteur).

## Tests

- **Unit (Python)** : requête CSV avec/sans filtre k-anon (bon `min_occurrences`) ; requête
  de stats (count_total/exposed/removed + ventilations) ; garde « dataset vide » ; garde de
  complétude du mois ; description FR à partir de stats connues.
- **Spike (bloquant)** : `datagouv_client` valide upload nouvelle resource + set description
  + auth + erreurs, contre un dataset de test.
- **Contrat (bloquant)** : diff à trois voies (publié en ligne / API actuelle / datalake) sur
  un mois settled.
- **E2E staging** : run → resource uploadée sur dataset de test + métadonnée + rapport écrit
  dans `datagouv/logs/`.

## Risques

1. **Maturité de la lib** — pré-1.0, faible adoption ; mitigé par le spike + fallback portage.
2. **Perf de la vue sur gros mois** — `export_opendata` non matérialisée + jointure FDW
   positions ; accepté (mensuel, lent toléré), CSV payé une fois ; stats déportées sur une
   requête allégée pour ne pas re-scanner la vue.
3. **Fraîcheur au bord du mois** — un run avant matérialisation des agrégats du mois donnerait
   des compteurs faux → garde de complétude.
4. **Migration des credentials** — `APP_DATAGOUV_KEY` etc. vers le secret datalake ; risque
   d'upload accidentel en test → garder `enabled`/`upload` en flags, dataset de test d'abord.
5. **Egress réseau** — le datalake (infra séparée) doit joindre data.gouv.fr (confirmer route
   + TLS) et le bucket S3.

## Hors périmètre

- Export à la demande (déjà migré).
- Matérialisation de `zone_aggregated` / `zone_exposed` en prod (chantier backfill séparé).
- Refonte des colonnes / du contenu du jeu de données open-data (migration iso-contrat).

## Points ouverts pour la planification

- Résultat du spike `datagouv_client` (retenue vs portage).
- Source exacte de `min_occurrences` (config datalake ; défaut 6) et confirmation que la
  vue expose bien les compteurs au grain attendu par le filtre.
- Confirmation de la stratégie de resource (nouvelle par mois vs rolling) contre le dataset
  réel.
- Schéma précis du rapport JSON `datagouv/logs/`.
- Notification de fin (les `TODO notify users` actuels) : conservée ou abandonnée (YAGNI ?).
