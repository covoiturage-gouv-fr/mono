---
name: apdf-checker
description: Use when verifying the monthly APDF (appels de fonds) files before publishing them - triggers on "vérifie les APDF", "check APDF du mois", "APDF checker", "vérifier les appels de fonds", or the recurring "Publication des APDF <mois>" task (run mensuel, le 6 du mois). Downloads the month's XLSX from the S3 bucket, runs 3 coherence checks with DuckDB, interprets the warnings, and consigns the result in the Notion ticket.
allowed-tools: Read, Write, Bash, Skill, mcp__claude_ai_Notion__notion-fetch, mcp__claude_ai_Notion__notion-search, mcp__claude_ai_Notion__notion-update-page
---

# APDF Checker

Vérifie les fichiers d'appels de fonds (APDF) d'un mois avant leur publication pour les
territoires et les opérateurs. Rejoue chaque mois le contrôle décrit dans le runbook
[Publier et vérifier les APDF chaque mois](https://app.notion.com/p/0f9281e30472418ca0e6656dde6661e2).

## Contexte métier

- Chaque nuit, `campaign:apply` calcule l'incitation théorique (*stateless*, champ
  `policy.incentives.result`), puis `campaign:finalize` applique les seuils/contexte
  (*stateful*, champ `policy.incentives.amount`). Une incitation à 0 en fin de mois =
  souvent **enveloppe consommée** (le `finalize` plafonne), pas une perte de données.
- Le 6 du mois, `apdf:export` génère un XLSX par campagne × opérateur actif et l'uploade
  dans le bucket S3. On vérifie **avant** de publier.

## Constantes

- Bucket : `api.production-appels-de-fonds`, alias `mc` local = **`dlk`**
  (chemin objet : `<campaign_id>/APDF-<YYYY-MM>-*.xlsx`).
- Outils : `mc` (minio-client) et `duckdb`. Si absents : `nix shell nixpkgs#minio-client nixpkgs#duckdb`.
- Page campagne : `https://app.covoiturage.beta.gouv.fr/campaign/<campaign_id>`
- Ticket Notion mensuel : tâche **« Publication des APDF <mois> <année> »**
  (base « Tâches & Planning par projet », priorité *Run mensuel*).

### Décodage du nom de fichier

`APDF-<YYYY>-<MM>-<campaign>-<operator>-<total>-<incited>-<amount_cents>-<slug>.xlsx`

- `total` = nb de trajets, `incited` = trajets incités, `amount_cents` = montant en **centimes**.
- Ces 3 champs = la page de synthèse générée par l'export : ils servent de **référence**
  à comparer aux lignes de détail.
- ⚠️ Le `slug` peut contenir des `-` (ex. `cotentin_2023-2024`) : parser les champs
  **depuis la gauche** (cut -f1..5), jamais depuis la droite.

### Structure du XLSX

- Feuille de détail = **`Trajets`** (une ligne par trajet).
- Lire avec `header=false, all_varchar=true` (l'auto-détection d'en-tête de DuckDB
  échoue sur ces fichiers), puis référencer les colonnes par position :
  - `D1` = `start_datetime` (ISO, ex. `2026-06-01T07:37:08+02:00`) → date = `substr(D1,1,10)`
  - `R1` = `rpc_incentive` en **euros** (décimal). `× 100` pour comparer aux centimes du nom.
- Filtrer les lignes avec `WHERE D1 LIKE '<YYYY>-%'` : ça exclut la ligne d'en-tête et
  d'éventuels trajets hors mois. Utiliser `TRY_CAST(R1 AS DOUBLE)` (l'en-tête casse le CAST).

## Procédure

Prendre le mois cible en argument (`YYYY-MM`), sinon le **mois précédent**.
Suivre les étapes avec une todo par item.

### 1. Lister et télécharger

```bash
MONTH=2026-06   # à adapter
DIR=$(mktemp -d "/tmp/apdf-$MONTH.XXXXXX")   # dossier isolé, chemin non prévisible
echo "Dossier de travail : $DIR"

# lister
mc ls -r dlk/api.production-appels-de-fonds/ | grep "APDF-$MONTH" | sort

# télécharger UNIQUEMENT le mois cible (ne pas faire un `mc cp --recursive` du bucket entier :
# il tire tout l'historique, plusieurs Go)
mc ls -r dlk/api.production-appels-de-fonds/ \
  | grep -oE "[0-9]+/APDF-$MONTH-[^ ]+\.xlsx" \
  | while read -r key; do
      dest="$DIR/$key"; mkdir -p "$(dirname "$dest")"
      [ -f "$dest" ] || mc cp "dlk/api.production-appels-de-fonds/$key" "$dest" >/dev/null 2>&1
    done
find "$DIR" -name "APDF-$MONTH-*.xlsx" | wc -l
```

Le fichier IDFM (`covoit_idfm`, campagne 1111) fait ~80 Mo : le téléchargement prend du temps.

### 2. Extraire les agrégats par jour

Produit un CSV `perday.csv` (une ligne par fichier × jour).

```bash
cd "$DIR"
OUT="$DIR/perday.csv"
echo "file,campaign,operator,total_fn,incited_fn,amount_cents_fn,day,trips,incited_day,inc_cents_day" > "$OUT"
for f in $(find . -name "APDF-$MONTH-*.xlsx" | sort); do
  base=$(basename "$f"); rest=${base#APDF-$MONTH-}
  camp=$(echo "$rest"|cut -d- -f1); op=$(echo "$rest"|cut -d- -f2)
  tot=$(echo "$rest"|cut -d- -f3); inc=$(echo "$rest"|cut -d- -f4); amt=$(echo "$rest"|cut -d- -f5)
  # garde-fou : champs numériques validés avant interpolation SQL (nom inattendu ignoré)
  if ! [[ "$camp$op$tot$inc$amt" =~ ^[0-9]+$ ]]; then
    echo "!! nom de fichier inattendu, ignoré : $base" >&2; continue
  fi
  Y=${MONTH%-*}
  duckdb -noheader -csv -c "LOAD excel;
    WITH t AS (SELECT substr(D1,1,10) d, TRY_CAST(R1 AS DOUBLE) i
               FROM read_xlsx('$f', sheet='Trajets', header=false, all_varchar=true)
               WHERE D1 LIKE '$Y-%')
    SELECT '$base','$camp','$op',$tot,$inc,$amt, d,
           count(*), count(*) FILTER(i>0), CAST(round(sum(i)*100) AS BIGINT)
    FROM t GROUP BY d ORDER BY d;" >> "$OUT"
done
```

### 3. Les 3 checks

```bash
cd "$DIR"
duckdb -c "CREATE TABLE pd AS SELECT * FROM read_csv('perday.csv', header=true);

-- CHECK 1 · Cohérence des montants : synthèse (nom de fichier) == somme du détail
SELECT campaign, operator,
       any_value(total_fn)=sum(trips) AS ok_trajets,
       any_value(incited_fn)=sum(incited_day) AS ok_incites,
       any_value(amount_cents_fn)=sum(inc_cents_day) AS ok_montant
FROM pd GROUP BY campaign, operator ORDER BY campaign, operator;"

# CHECK 2 (jours couverts) + CHECK 3 (continuité) : adapter range(1,32) au nb de jours du mois
duckdb -c "CREATE TABLE pd AS SELECT * FROM read_csv('perday.csv', header=true);
WITH days AS (SELECT format('$MONTH-{:02d}', g) d FROM range(1,31) t(g)),
     files AS (SELECT DISTINCT file, campaign, operator, total_fn FROM pd),
     grid AS (SELECT f.*, dd.d, p.trips, p.inc_cents_day
              FROM files f CROSS JOIN days dd
              LEFT JOIN pd p ON p.file=f.file AND p.day::VARCHAR=dd.d)
SELECT campaign, operator, any_value(total_fn) trajets,
       list(substr(d,9,2)) FILTER(trips IS NULL OR trips=0)                       AS jours_manquants,
       list(substr(d,9,2)) FILTER(trips>0 AND (inc_cents_day IS NULL OR inc_cents_day=0)) AS jours_incitation_zero
FROM grid GROUP BY campaign, operator ORDER BY campaign, operator;"
```

Pour caractériser une incitation à 0 (check 3), sortir le détail jour par jour avec le cumul :

```bash
duckdb -c "CREATE TABLE pd AS SELECT * FROM read_csv('$DIR/perday.csv', header=true);
SELECT day, trips, incited_day, round(sum(inc_cents_day) OVER (ORDER BY day)/100.0,2) cumul_eur
FROM pd WHERE campaign=<ID> ORDER BY day;"
```

## Interprétation (OK / WARN / ERROR)

- **Check 1 — cohérence** : un écart trajets/incités/montant = **ERROR** (fichier incohérent,
  ne pas publier, investiguer l'export). L'égalité stricte est attendue.
- **Check 2 — jours manquants** :
  - Sur **petites campagnes** → **WARN** seulement. Vérifier que les trous tombent sur les
    **week-ends** (`dayname('<date>'::DATE)`) : un service en jours ouvrés (ex. lignes Ecov)
    n'a normalement pas de trajets le samedi/dimanche → attendu.
  - Un trou en semaine sur une grosse campagne → **investiguer**.
- **Check 3 — incitation qui passe à 0** → **WARN**. Cause la plus fréquente = **enveloppe
  mensuelle consommée** : le cumul se fige exactement au montant du fichier et les trajets
  continuent d'être enregistrés avec incitation 0. C'est le comportement attendu du `finalize`
  (cf. runbook « vérifier si l'enveloppe est consommée »). Si le cumul ne correspond pas au
  plafond, ou si tout un opérateur passe à 0 d'un coup sans plafond atteint → investiguer un
  `apply`/`finalize` incomplet (relancer apply → reset → finalize → `campaign:sync`).

## Consigner dans Notion

1. `notion-search` la tâche « Publication des APDF <mois> <année> » (vérifier `userDefined:ID`
   = le GEN attendu). Runbook général : `0f9281e30472418ca0e6656dde6661e2`.
2. Invoquer le skill `french` (français correct, accents, y compris sur les majuscules).
   **Undercover** : aucune mention de Claude/IA. **Dépôt/contenu public exclu** — Notion est
   interne, les montants y sont admis.
3. `notion-update-page` en `insert_content` `{"type":"end"}` : **ajouter** une section datée,
   ne jamais écraser. Structurer par les 3 checks + un bilan en callout.
4. Lier chaque campagne citée : `[<id> · <slug>](https://app.covoiturage.beta.gouv.fr/campaign/<id>)`.
5. Action sortante : montrer le contenu et **confirmer avant d'écrire** dans Notion.

## Nettoyage (dernière étape, sur validation)

Les XLSX téléchargés contiennent des données trajets (dates, incitations) qui transitent en
clair dans `/tmp`. C'est la **toute dernière étape**, à ne faire **qu'une fois que tout est OK
et validé par l'utilisateur** : contrôle consigné dans Notion **et** APDF jugés bons/publiables.
Ne jamais supprimer avant cette validation — on peut avoir besoin de re-creuser un fichier.

Demander confirmation, puis supprimer le répertoire de travail :

```bash
rm -rf "$DIR"   # /tmp/apdf-<mois>.XXXXXX
```

## Pièges connus

- `mc cp --recursive` sur le bucket = tout l'historique (plusieurs Go). Toujours filtrer par mois.
- `duckdb` lit l'en-tête comme données → `TRY_CAST` + `WHERE D1 LIKE '<Y>-%'`, jamais `OFFSET 1`
  (l'OFFSET est appliqué après la projection, donc le CAST plante sur la ligne d'en-tête).
- Le nom de fichier se parse depuis la gauche (les slugs contiennent des `-`).
- Adapter `range(1,31)` du check 2 au nombre de jours du mois (+1) : 30 j → `range(1,31)`,
  31 j → `range(1,32)`, février → `range(1,29/30)`.
