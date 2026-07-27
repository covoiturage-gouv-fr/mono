# Mode debug + verdict de cohérence pour l'export data.gouv

Date : 2026-07-17
Statut : design validé, prêt pour plan d'implémentation
Ticket parent : GEN-634 (Fixes Opendata)

## Contexte

Le job `datalake/pipelines/cmd/datagouv.py` publie chaque mois le CSV open-data sur
data.gouv.fr, avec une description contenant des compteurs (total, exposés, retirés,
ventilation géographique France/Étranger).

Deux limites actuelles pour debugguer un export sur de **vraies** données :

1. Le dry-run existant (sans `APP_DATAGOUV_UPLOAD`) génère le CSV dans un
   `TemporaryDirectory` puis le **jette** : impossible d'inspecter le fichier produit.
2. Il n'existe aucun **contrôle de cohérence** automatique entre le CSV, les compteurs
   et la description. La vérification est un travail d'œil humain (rouvrir le CSV,
   recompter, comparer). C'est précisément ce qui a permis de laisser passer l'écart
   `total ≠ exposés + retirés` corrigé par ailleurs.

## Objectif

Un mode `--debug` sur `just datagouv`, lancé **manuellement dans le pod datalake** sur de
vraies données, qui :

- **ne publie jamais** sur data.gouv ;
- **matérialise** les artefacts réels (CSV, description, rapport) sur le S3 du datalake,
  horodatés, pour inspection ;
- **calcule des invariants de cohérence** et imprime un **verdict** (PASS/FAIL/WARN par
  check), consigné dans les artefacts.

## Non-objectifs (YAGNI)

- Pas d'automatisation du run réel en CI : la CI n'a pas de données pertinentes. Seule la
  **logique de checks** (module pur) est testée en CI sur fixtures synthétiques.
- Pas de blocage de la publication réelle pour l'instant. Le module de checks est écrit
  pour être **réutilisable** plus tard en garde-fou bloquant avant publication, mais on
  ne l'active pas ici.
- Pas de contrôles « métier » (volumétrie vs mois précédents, valeurs aberrantes,
  complétude amont) : ils relèvent de l'observabilité continue et sont consignés dans la
  carte Notion **GEN-660 « Alerting Elementary »**, section datagouv.

## Surface de commande

Nouveau flag sur la commande Typer `run` :

```
just datagouv --debug            # + --start / --end / --min-occurrences existants
```

Comportement en `--debug` :

- Force `upload data.gouv = non`, quel que soit `APP_DATAGOUV_UPLOAD`.
- Reste gaté par `APP_DATAGOUV_ENABLED` (comme aujourd'hui).
- Matérialise CSV + description + rapport sur S3 (cf. ci-dessous).
- Calcule et imprime le verdict de cohérence.
- **Un `FAIL` n'interrompt pas le dump** (on veut justement les fichiers quand ça casse),
  mais la commande sort en **exit 1** pour le signal. Les `WARN` n'affectent pas l'exit.

## Artefacts S3

Tous sous le préfixe existant `datagouv/logs/`, partageant un même horodatage
`<ts>` au format UTC compact `YYYYMMDDTHHMMSSZ` calculé une fois au début du run.

| Artefact | Chemin | Note |
| --- | --- | --- |
| Rapport JSON | `datagouv/logs/<mois>-<ts>.json` | Aujourd'hui `<mois>.json`, écrasé à chaque run → **désormais horodaté** (subsume la todo « timestamp log »). Enrichi de `mode: "debug"` et d'un bloc `checks`. |
| CSV | `datagouv/logs/<mois>-<ts>-debug.csv` | Le CSV **exactement** tel qu'il serait publié (même `COPY`, même k-anonymat). |
| Description | `datagouv/logs/<mois>-<ts>-debug.md` | La description data.gouv + le tableau de verdict. |

Confidentialité : le `-debug.csv` est le fichier qui serait publié en open-data (contenu
public, k-anonymisé). Il reste dans le bucket **privé** du datalake → aucune exposition
nouvelle vs la publication.

Le run **réel** (hors `--debug`) conserve le rapport horodaté `<mois>-<ts>.json` lui aussi,
pour ne plus écraser l'historique des publications.

## Checks de cohérence

Deux niveaux :

- `FAIL` — invariant dur cassé (le CSV/la description ne se recoupent pas).
- `WARN` — écart connu ou toléré (informationnel, n'échoue pas la commande).

### Sur les stats seules

- `FAIL` si `count_total ≠ count_exposed + count_removed`.
- `FAIL` si `count_removed ≠ count_removed_start + count_removed_end - count_removed_both`.
- `FAIL` si `count_exposed_france_france + _france_etranger + _etranger_etranger ≠ count_exposed`.

### CSV ↔ stats

- `FAIL` si le nombre de lignes de données du CSV ≠ `count_exposed`.
- `FAIL` si la ventilation géographique **recalculée depuis les colonnes INSEE du CSV**
  (préfixe `99` = étranger) ≠ `(count_exposed_france_france, _france_etranger, _etranger_etranger)`.

### CSV interne

- `FAIL` si la première ligne ≠ en-tête du contrat (`csv_header()`).
- `FAIL` si une ligne étrangère (INSEE `99xxx`) a `department` / `town` / `towngroup`
  non vides (le contrat les veut vides pour l'étranger).
- `WARN` : nombre d'inversions du tri sur `journey_start_datetime`. Attendu `> 0` tant que
  le décalage de fuseau DROM (todo dédiée) n'est pas réglé — le check le **quantifie**
  au lieu de le masquer.

### Hors périmètre des checks CSV

Le k-anonymat **par ligne** n'est pas vérifiable depuis le CSV publié : les colonnes
`start_insee_count` / `end_insee_count` servent au filtre `WHERE` du `COPY` mais ne font
pas partie du contrat de colonnes. Il est structurellement garanti par ce `WHERE` et
indirectement couvert par « nb lignes = count_exposed ».

## Structure de code

Nouveau module pur `datalake/pipelines/helpers/datagouv_checks.py` :

```python
@dataclass(frozen=True)
class CheckResult:
    name: str
    level: str      # "FAIL" | "WARN"
    ok: bool
    detail: str

def run_checks(stats: dict, csv_path: str) -> list[CheckResult]: ...
def render_markdown(results: list[CheckResult]) -> str: ...   # tableau pour le .md
def has_failure(results: list[CheckResult]) -> bool: ...      # -> exit code
```

- **Aucun accès réseau/DB** : `run_checks` prend le `stats` déjà calculé et lit le CSV sur
  disque (streaming ligne à ligne — le fichier fait ~40 Mo). → **testable en CI**.
- Découpage : chaque invariant est une petite fonction interne renvoyant un `CheckResult`,
  `run_checks` les agrège. Ajouter un check = ajouter une fonction + une entrée.

Modifications de `datalake/pipelines/cmd/datagouv.py` :

- Ajouter le flag `--debug` à `run(...)`.
- Calculer `<ts>` une fois ; horodater le rapport JSON (réel **et** debug).
- En `--debug` : après `stream_csv`, uploader le CSV et la description sur S3, appeler
  `run_checks`, imprimer le verdict, injecter `checks` dans le rapport, fixer l'exit code.
- `build_report` gagne un champ `mode` et un champ `checks`.

`datalake/justfile` : la recette `datagouv` passe déjà les arguments — `just datagouv --debug`
fonctionne sans changement, à vérifier.

## Tests

- `datalake/tests/test_datagouv_checks.py` (nouveau, **pur**, tourne en CI) :
  - chaque `FAIL` déclenché par un `stats`/CSV incohérent monté à la main ;
  - cas nominal tout-vert ;
  - `WARN` de tri : CSV volontairement désordonné → inversions comptées ;
  - `has_failure` / `render_markdown`.
- Réutiliser le style de fixtures synthétiques de `test_datagouv_output.py` (CSV écrit dans
  un `tmp_path`, `stats` en dict).
- Pas de test de bout en bout du run `--debug` (nécessite S3 + vraies données ; hors CI).

## Séquencement

1. Module `datagouv_checks.py` + tests (TDD, CI-vert).
2. Horodatage du rapport (réel + debug) + flag `--debug` + dump S3 + câblage du verdict.
3. (Séparé) Enrichir la carte GEN-660 « Alerting Elementary » avec la section datagouv.
