---
name: apdf-deltas
description: Use when an operator or a territory reports a gap between the incentives the operator declared and the amounts in an APDF (appel de fonds) - triggers on "écart APDF", "deltas d'appel de fonds", "analyse les écarts d'incitation", "l'opérateur trouve plus de trajets que l'APDF", "pourquoi ces trajets ne sont pas financés", or a Zammad/Notion ticket about APDF amounts. Classifies each trip of a campaign × month in the datalake (déclaré opérateur vs calculé RPC), explains each category against the campaign handler rules, and writes a synthesis note (method, detailed conclusions, short email for the territory and the operator).
allowed-tools: Read, Write, Bash, Grep, Glob, mcp__claude_ai_Notion__notion-fetch, mcp__claude_ai_Notion__notion-search, mcp__claude_ai_Notion__notion-update-page, mcp__claude_ai_Notion__notion-create-pages, mcp__claude_ai_Notion__notion-create-file-upload, mcp__claude_ai_Notion__notion-create-attachment
---

# Analyse des écarts d'APDF

Explique l'écart entre ce qu'un opérateur a déclaré (`incentives[]` envoyé à l'API, par SIRET
financeur) et ce que le RPC a calculé pour une campagne sur un mois, trajet par trajet.
Travaille sur le datalake (`datalake_production`), en lecture seule.

Complément de `apdf-checker` : celui-là vérifie la cohérence interne des fichiers avant
publication ; celui-ci explique un écart signalé après publication.

## Contexte métier

| Côté | Table datalake | Contenu |
| ---- | -------------- | ------- |
| Déclaré | `zone_trusted.operator_incentives` | `(carpool_id, siret, amount)` envoyé par l'opérateur, montants > 0 seulement |
| RPC | `zone_trusted.incentives` | une ligne par trajet × campagne : `result` (stateless, règles d'éligibilité et barème), `amount` (stateful, après plafonds), `status` |
| Trajet | `zone_trusted.carpools` | classe, distance, contribution, places, codes INSEE, statuts acquisition / fraude / anomalie |

- Le moteur écrit une incitation à **0** pour un trajet inéligible (`NotEligibleTargetException`)
  et **aucune ligne** si le trajet ne passe pas le pré-filtre de périmètre de la campagne
  (`Policy.guard`) ou si un bout n'a pas de territoire (code INSEE obsolète).
- `result > amount` = plafond stateful : limite par personne et par jour ou enveloppe consommée
  (`campaign:finalize`).
- L'APDF (`api/src/pdc/services/apdf/providers/DataRepositoryProvider.ts`) retient :
  `pi.status = 'validated'`, `acquisition_status IN (processed, canceled, updated)`,
  `fraud_status IN (passed, failed)`. Le déclaré y est rattaché au territoire par
  **SIREN** (`left(siret, 9)`), comme dans `deltas.sql`.
- Le mois est en **heure locale** (`start_datetime_tz`) : un trajet du 31 à 23 h 30 à Paris est
  le 1er en UTC. Garder l'heure locale pour coller à l'APDF.

## Constantes

- Connexion : variables `DBT_*` de `datalake/.env` (tunnel local `localhost:54321`).
  Si la connexion est refusée, **demander à l'utilisateur d'ouvrir le tunnel** ; ne pas viser
  une autre base.
- Toujours en lecture seule : `PGOPTIONS='-c default_transaction_read_only=on'`.
- Handlers de campagne : `api/src/pdc/services/policy/engine/policies/` ; le lien
  campagne → fichier passe par `dlk_import.policy_policies.handler` = `static readonly id`.
- Page campagne : `https://app.covoiturage.beta.gouv.fr/campaign/<campaign_id>`

## Procédure

Une todo par étape.

### 0. Cadrer

Relever dans le ticket (Notion, Zammad) : campagne(s), opérateur, mois, zone concernée,
chiffres de l'opérateur, dates de bascule annoncées. Si la campagne n'est pas donnée,
la chercher :

```sql
SELECT _id, name, start_date, end_date, status, territory_name, territory_siret
FROM zone_trusted.policies WHERE name ILIKE '%occitanie%' ORDER BY start_date DESC;
```

Plusieurs campagnes peuvent se partager un trajet (ex. région + agglomération) : analyser
chaque campagne citée, puis croiser.

### 1. Lire les règles de la campagne

```bash
cd datalake && set -a && . ./.env && set +a
export PGPASSWORD=$DBT_PASSWORD PGOPTIONS='-c default_transaction_read_only=on'
PSQL="psql -h $DBT_HOST -p $DBT_PORT -U $DBT_USER -d $DBT_DBNAME"
$PSQL -c "SELECT _id, handler, max_amount, start_date, end_date FROM dlk_import.policy_policies WHERE _id = <ID>"
grep -rl 'static readonly id = "<handler>"' ../api/src/pdc/services/policy/engine/policies/
```

Lire le handler et son `.data.ts` : opérateurs et dates, classes, tranches de distance,
contribution min/max, périmètre (`startsAndEndsAt`, AOM internes exclues…), jours exclus,
périodes booster, limites (`watchForPersonMaxTripByDay`, `watchForGlobalMaxAmount`).
Noter aussi `git log` du handler : une règle changée en cours de mois explique souvent un écart.
Écrire la liste des règles testables en SQL (étape 3).

### 2. Classer les trajets

`deltas.sql` sort un CSV (une ligne par trajet, catégorie calculée) ; l'analyse se fait
ensuite en local avec DuckDB. La lecture seule interdit les tables temporaires, et l'extract
coûte **5 à 10 min** : l'extraire une fois, itérer en local.

```bash
DIR=<scratchpad>/apdf-deltas-<campagne>-<mois>; mkdir -p "$DIR"
$PSQL -X -q -v camp=<ID> -v op=<OPERATOR_ID ou 0> -v start=2026-09-01 -v end=2026-11-01 \
      -v zone_col=epci -v zone_code=<SIREN ou ''> \
      -f ../.claude/skills/apdf-deltas/deltas.sql -o "$DIR/trajets.csv"
```

`zone_col` ∈ `com | epci | aom | dep | reg | arr` ; `zone_code=''` = pas de filtre de zone.
Prendre plusieurs mois d'un coup quand une date de bascule est en jeu (le mois d'avant sert
de témoin).

```bash
cd "$DIR" && duckdb -c "CREATE TABLE d AS SELECT * FROM read_csv('trajets.csv', header=true);
SELECT strftime(day, '%Y-%m') mois, count(*) trajets, count(*) FILTER (declared > 0) declares,
       count(*) FILTER (rpc_amount > 0 AND rpc_status = 'validated') finances,
       round(sum(declared) / 100, 2) declare_eur,
       round(sum(rpc_amount) FILTER (rpc_status = 'validated') / 100, 2) rpc_eur
FROM d GROUP BY 1 ORDER BY 1;
SELECT strftime(day, '%Y-%m') mois, categorie, count(*) n, round(sum(declared) / 100, 2) declare_eur,
       round(sum(rpc_result) / 100, 2) result_eur, round(sum(rpc_amount) / 100, 2) amount_eur
FROM d GROUP BY ALL ORDER BY 1, n DESC;"
```

Catégories (1re règle qui s'applique) :

| Catégorie | Sens | Piste |
| --------- | ---- | ----- |
| `absent_rpc` | aucune incitation RPC pour cette campagne | hors périmètre (`guard`), code INSEE inconnu, trajet arrivé après le dernier `apply` |
| `hors_apdf_statut` | statut d'acquisition ou de fraude exclu de l'APDF | `acquisition_status`, `fraud_status` |
| `non_valide` | incitation pas `validated` | mois pas finalisé |
| `fraude_anomalie` | dans l'APDF mais fraude/anomalie ≠ `passed` | `fraud_labels`, `anomaly_labels` |
| `inegible_rpc` | déclaré > 0, `result` = 0 | règle d'éligibilité du handler (étape 3) |
| `plafonne` | `result` > `amount` | limite journalière ou enveloppe |
| `non_declare` | financé RPC, rien déclaré | l'opérateur n'a pas déclaré ce SIRET |
| `montant_different` | les deux > 0, montants différents | barème, booster, contribution déduite |
| `ok` | concordant | |

Vérifier d'abord que les totaux de la vue d'ensemble collent aux chiffres de l'opérateur et
de l'APDF publié (`apdf-checker` sait lire le XLSX). Sinon le périmètre de l'analyse est faux :
corriger avant d'interpréter.

### 3. Expliquer chaque catégorie

**`absent_rpc` : commencer par le périmètre de la campagne.** `Policy.guard` n'évalue un
trajet que si son départ **ou** son arrivée est dans le périmètre (versions de
`territory_perimeters` à la date du trajet, sinon sélecteurs du territoire porteur). Hors
périmètre, le moteur n'écrit aucune ligne, même si le handler juge le trajet éligible.

```bash
$PSQL -c "SELECT territory_id FROM dlk_import.policy_policies WHERE _id = <ID>" \
      -c "SELECT * FROM dlk_import.territory_territory_group_selector WHERE territory_group_id = <TID>" \
      -c "SELECT version, valid_from, valid_to, cardinality(arr) FROM dlk_import.territory_territory_perimeters WHERE territory_id = <TID>"
```

Puis regarder où vont les trajets absents, par AOM / EPCI de l'autre extrémité et par jour :

```sql
SELECT categorie, CASE WHEN zone_start THEN end_l_aom ELSE start_l_aom END AS autre_aom, count(*) n
FROM d GROUP BY ALL ORDER BY n DESC;
SELECT day, count(*) FILTER (categorie = 'absent_rpc') absents, count(*) FILTER (categorie <> 'absent_rpc') avec_rpc
FROM d GROUP BY 1 ORDER BY 1;
```

Une coupure nette (tous les absents hors du périmètre, à partir d'une date) désigne le
périmètre, pas une règle du handler. Mesurer alors l'ampleur hors zone : trajets déclarés au
SIREN du territoire sans ligne RPC, par mois et opérateur (`operator_incentives` ⋈
`incentives`, ~5 min sur 10 mois).

Pour `inegible_rpc` et le reste des `absent_rpc`, tester les règles du handler une à une.
Exemple (Occitanie 2026) :

```sql
SELECT
  CASE
    WHEN passenger_contribution < 50 THEN 'contribution < 0,50 €'
    WHEN day BETWEEN '2026-10-01' AND '2026-10-31' AND passenger_contribution > 200 THEN 'booster : contribution > 2 €'
    WHEN operator_class NOT IN ('B', 'C') THEN 'classe ' || operator_class
    WHEN distance < 2000 THEN 'distance < 2 km'
    WHEN distance > 50000 THEN 'distance > 50 km'
    WHEN start_reg IS DISTINCT FROM '76' OR end_reg IS DISTINCT FROM '76' THEN 'départ ou arrivée hors région'
    WHEN start_aom = end_aom THEN 'interne à une AOM : ' || start_l_aom
    ELSE 'autre (limite, distance 30-50 km hors bassin…)'
  END AS motif,
  count(*), sum(declared) AS declare_cts
FROM d WHERE categorie IN ('inegible_rpc', 'absent_rpc')
GROUP BY 1 ORDER BY 2 DESC;
```

Pour `plafonne` : compter les trajets du même `driver_key` / `passenger_key` le même jour
et comparer au plafond ; pour l'enveloppe, suivre le cumul de `rpc_amount` par jour.

Croiser avec la zone et le temps : `zone_start`/`zone_end` (entrant, sortant, interne),
`day` (date de bascule annoncée), `isodow`. Contrôler 3 à 5 trajets à la main jusqu'au bout
(règle par règle) avant de conclure.

Si une règle du handler ne correspond pas à la convention ou au ticket (bascule non
paramétrée, date de fin, AOM listée à tort), le dire nettement : c'est une correction côté
RPC, avec recalcul (`apply` → `finalize`) et APDF à régénérer pour les mois touchés.

### 4. Rédiger la note

Structure :

1. **Contexte** : demande, campagne(s) liée(s), opérateur, mois, zone, chiffres annoncés.
2. **Méthode** : sources datalake, rattachement par SIREN, heure locale, catégories,
   règles testées (citer le handler).
3. **Résultats** : tableau par catégorie (trajets, déclaré, RPC, delta en euros),
   puis détail des motifs, par zone et par jour si utile.
4. **Conclusions** : causes classées par poids dans l'écart ; ce qui est attendu (règle de
   la convention) vs ce qui est une erreur (RPC ou opérateur) ; actions (correction de
   handler, recalcul, APDF à régénérer, déclaration à corriger).
5. **Message à envoyer** : 5 à 10 lignes, chiffres agrégés seulement, sans jargon technique
   (pas de `result`/`amount`, pas de nom de table), une phrase par cause, la suite donnée.

Règles :

- Montants en euros dans la note (les tables sont en centimes).
- **Aucune donnée personnelle** dans le message à envoyer ni dans Notion (note et annexe) :
  pas de `driver_key`, `passenger_key`, identifiant utilisateur, téléphone.
  `operator_journey_id` est admis (identifiant de trajet chez l'opérateur, sert au
  rapprochement).
- Le détail trajet par trajet est joint en annexe CSV (étape 5), pas recopié dans la note.
- Français correct, accents compris sur les majuscules. Aucune mention de Claude/IA.

### 5. Consigner

1. Construire l'annexe CSV depuis le CSV de travail, **sans `driver_key` ni
   `passenger_key`** (identifiants de personnes) :

   ```bash
   cd "$DIR" && duckdb -c "COPY (
     SELECT operator_name, operator_journey_id, start_datetime_tz, categorie,
            declared / 100.0 AS declare_eur, rpc_result / 100.0 AS rpc_result_eur,
            rpc_amount / 100.0 AS rpc_amount_eur, rpc_status,
            operator_class, distance, passenger_seats, passenger_contribution / 100.0 AS contribution_eur,
            start_geo_code, start_l_arr, start_l_aom, end_geo_code, end_l_arr, end_l_aom,
            zone_start, zone_end, acquisition_status, fraud_status, anomaly_status
     FROM read_csv('trajets.csv', header=true) ORDER BY start_datetime_tz
   ) TO 'annexe-<campagne>-<période>.csv' (HEADER);"
   ```

   Si l'étape 3 a calculé un motif par trajet, l'ajouter en colonne `motif`.
2. Montrer la note et l'annexe (nb de lignes, colonnes) à l'utilisateur et **confirmer avant
   d'écrire** dans Notion.
3. Téléverser l'annexe : `notion-create-file-upload` (`filename` = nom du CSV), puis un seul
   POST multipart vers `upload_url` avec tous les `upload_headers` :
   `curl -sS -X POST "<upload_url>" -H "<header>: <valeur>" … -F "file=@annexe-….csv"`
   (limite 20 Mio). Garder le `markdown_source` renvoyé.
4. `notion-update-page` sur la tâche du ticket, `insert_content` en fin de page : ajouter une
   section datée, ne jamais écraser, avec l'annexe en fin de section
   (`<file source="file-upload://…" />`). Lier chaque campagne :
   `[<id> · <nom>](https://app.covoiturage.beta.gouv.fr/campaign/<id>)`.
5. Le message à envoyer est rendu aussi dans la réponse, prêt à coller ; ne jamais l'envoyer
   soi-même. L'annexe reste interne : la transmettre à l'opérateur est une décision de
   l'utilisateur.
6. Une fois la note validée, proposer de supprimer `$DIR` (données trajets en clair).

## Pièges connus

- Lecture seule : `CREATE TEMP TABLE` est refusé aussi. D'où l'extract CSV + DuckDB.
- Un opérateur compte souvent autrement que nous (trajets, passagers, filtre de zone) :
  si ses chiffres ne collent pas à la vue d'ensemble, lui demander son critère avant de
  conclure plutôt que forcer la réconciliation.
- `operator_incentives` ne garde que `amount > 0` : un déclaré à 0 n'apparaît pas.
- Un opérateur peut déclarer sous un SIRET d'établissement différent de celui du territoire :
  le SIREN suffit. S'il déclare au nom d'un autre financeur (ex. la région au lieu de
  l'agglo), le trajet sort en `non_declare` côté campagne visée et ne figure pas dans son
  déclaré : regarder `oi_details` dans `zone_trusted.carpools` pour voir à qui il a déclaré.
- Les tables trusted sont rafraîchies chaque nuit avec 3 mois de recul : un recalcul fait le
  jour même en prod n'est visible que le lendemain.
- `incentives` peut porter plusieurs lignes par trajet × campagne (historique) :
  `deltas.sql` garde la plus récente.
- Le millésime géo utilisé est le dernier : une commune fusionnée depuis le trajet peut
  changer d'EPCI/AOM.
