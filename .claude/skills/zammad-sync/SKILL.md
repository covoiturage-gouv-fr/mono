---
name: zammad-sync
description: Use when reconciling open Zammad support tickets with the Notion task tracker ("synchronise mes tickets Zammad", "sync Zammad Notion", "quels tickets Zammad manquent dans Notion", "liste mes tickets Zammad ouverts et compare à Notion", "crée les tâches Notion manquantes pour mes tickets"). Lists the current user's open Zammad tickets, matches them against tasks in the "Suivi des tâches" database via the zammad_url property, surfaces the tickets missing in Notion, and creates the chosen ones as tasks.
allowed-tools: Bash, Skill, AskUserQuestion, mcp__zammad__zammad_get_current_user, mcp__zammad__zammad_search_tickets, mcp__zammad__zammad_get_ticket, mcp__claude_ai_Notion__notion-fetch, mcp__claude_ai_Notion__notion-search, mcp__claude_ai_Notion__notion-query-database-view, mcp__claude_ai_Notion__notion-create-pages
---

# Synchro tickets Zammad → Notion

Réconcilie les tickets de support **Zammad** ouverts de l'utilisateur courant
avec le suivi des tâches **Notion**. But : ne jamais perdre un ticket actif et
éviter les doublons. Le skill **liste les tickets manquants** dans Notion ;
**l'utilisateur décide** lesquels créer. Sens unique : Zammad → Notion (on ne
modifie pas les tickets Zammad).

## Cibles Notion (constantes)

- **Suivi des tâches** (où l'on lit et écrit les tâches) :
  `collection://2759b461-218e-4764-ae17-0025d728193c`
  - Database parente : `b358a631afcc4e7a9a0a39092bf2d953`
  - Propriété de correspondance : **`zammad_url`** (type URL) — clé canonique de
    rapprochement ticket ↔ tâche. Une tâche est « liée » à un ticket si son
    `zammad_url` pointe sur ce ticket (même **id interne**, voir plus bas).
  - Vue **« Tickets Zammad »** (filtre `zammad_url` non vide, tri `ID` décroissant)
    — source des tâches déjà liées :
    `https://www.notion.so/b358a631afcc4e7a9a0a39092bf2d953?v=37c994bec93181c4bb6b000cb952ef91`
- Utilisateur courant : résoudre dynamiquement.
  - **Zammad** : `zammad_get_current_user` → champ `login` (= email Zammad).
  - **Notion** : `git config user.email` puis `notion-search`
    (`query_type: "user"`, requête = cet email) → `user://…`. Ne jamais coder en
    dur d'identité (nom, ID, email).

## Identifiants Zammad : `id` vs `number`

Un ticket Zammad a deux nombres : son **`id` interne** (ex. `7618`) et son
**`number`** d'affichage (ex. `347614`). L'URL d'un ticket utilise l'**id
interne** :

```
https://covoiturage-betagouv.zammad.com/#ticket/zoom/<id>
```

C'est cette URL qu'on stocke dans `zammad_url`, et c'est sur l'**id interne**
qu'on rapproche. Ne jamais rapprocher sur le `number`.

## Règles transverses

- **Vues plutôt que pages** : pour collecter les tâches liées, passer par
  `notion-query-database-view` (filtres côté serveur, pas de corps de page).
  Éviter `notion-search` pour les sélections de tâches (classe par pertinence,
  rate des résultats). `notion-fetch` sur une page uniquement en dernier recours.
- **Action sortante** : Notion est externe. Toujours montrer la liste des
  manquants et **attendre la décision de l'utilisateur avant de créer** quoi que
  ce soit. Ne jamais créer en masse sans validation explicite.
- **Pas de doublon** : ne proposer que les tickets dont l'id interne n'apparaît
  dans aucun `zammad_url` existant (toutes tâches confondues, y compris `Done`).
- **Minimisation des données personnelles** : Notion ne reçoit qu'un **lien**
  Zammad (`zammad_url`) + un **libellé/contexte reformulés**. Ne jamais recopier
  dans Notion de données personnelles brutes du ticket (email ou téléphone du
  client, identité, contenu intégral d'un article). La PII reste dans Zammad,
  qui est l'outil contrôlé d'origine ; Notion ne stocke qu'une référence et un
  résumé métier. Le champ `customer` ne sert qu'au tableau de comparaison
  affiché en conversation (étape 4) — il n'est **pas** écrit dans Notion.
- **Français correct** (invoquer le skill `french` avant de rédiger les
  libellés), accents sur majuscules.
- **Undercover** : aucune mention de Claude / IA / assistant dans Notion.

## Procédure

### 1. Résoudre les deux identités

- Skill `french`.
- Zammad : `zammad_get_current_user` → `login`.
- Notion : `git config user.email` → `notion-search` (`query_type: "user"`) →
  `user://…`.

### 2. Lister les tickets Zammad ouverts

`zammad_search_tickets` avec `owner` = le `login` Zammad, `state` = `open`,
`per_page` = 100, `response_format` = `json`. Retenir pour chaque ticket :
`id` (interne), `number`, `title`, `customer`, `updated_at`.

> Périmètre par défaut : `owner = moi` (ce que je dois traiter). Si
> l'utilisateur veut un autre périmètre (`state: "new"`, tickets d'un groupe,
> etc.), adapter les filtres et le lui confirmer.

### 3. Lister les tâches Notion déjà liées

Interroger la vue **« Tickets Zammad »** avec `notion-query-database-view` (URL
dans les constantes). Elle filtre `zammad_url` non vide côté serveur.

- Extraire la propriété `zammad_url` de chaque tâche → en déduire l'**id
  interne** (suffixe après `/zoom/`).
- Construire l'ensemble `ids_liés` = tous les ids internes déjà présents.
- Paginer (`start_cursor` = `next_cursor`) tant que `has_more`.

Repli (vue absente / indisponible) : interroger la database
`b358a631afcc4e7a9a0a39092bf2d953` via `notion-fetch` pour retrouver une vue
listant les tâches, puis filtrer localement sur `zammad_url` non vide. En
tout dernier recours seulement, `notion-search` sur le numéro/sujet du ticket.

### 4. Comparer

Pour chaque ticket ouvert (étape 2) : **manquant** si son `id` ∉ `ids_liés`.

Présenter un tableau dans la conversation :

```markdown
| Ticket | Sujet | Client | Dans Notion ? |
| --- | --- | --- | --- |
| 347614 | … | … | ❌ manquant |
| 347622 | … | … | ✅ GEN-646 |
```

(Indiquer le `GEN-NNN` / l'URL Notion pour les tickets déjà liés.)

### 5. Laisser l'utilisateur décider

Lister explicitement les **manquants** et demander lesquels créer :
`AskUserQuestion` (ou texte libre) — « tous / aucun / les numéros ». Les
« numéros » sont les `number` affichés dans le tableau (ex. `347614, 347622`).
Ne rien créer si la réponse est « aucun ».

### 6. Créer les tâches choisies

Pour chaque ticket retenu, `notion-create-pages` :

- `parent` : `{ "type": "data_source_id", "data_source_id": "2759b461-218e-4764-ae17-0025d728193c" }`
- `properties` :
  - `Tâche` (titre) : libellé court `<opérateur> - <sujet>`, minuscules, sans le
    numéro de ticket (ex. `karos - trajets refusés limite quotidienne`).
    L'**opérateur** se déduit du client : nom de l'organisation Zammad du ticket,
    sinon domaine de l'email `customer` sans le TLD (`nicolas@karos.fr` → `karos`,
    `…@ecov.fr` → `ecov`, `…@karos-mobility.com` → `karos`). Si le rattachement
    est ambigu (adresse générique type gmail, plusieurs opérateurs), demander à
    l'utilisateur plutôt que deviner.
  - `zammad_url` : `https://covoiturage-betagouv.zammad.com/#ticket/zoom/<id>`
    (id **interne**) — c'est ce champ qui rend la synchro idempotente.
  - `Personne` : `["<id Notion utilisateur courant>"]` (JSON array).
  - `Priority` : `⏰ Medium` (défaut ; `🔥 High` ou `Bug` si le ticket le
    justifie).
  - `État` : `Tâches priorisées`.
- `content` : le lien Zammad sur sa propre ligne, suivi d'une ligne de contexte
  **reformulée** à partir du sujet/du dernier échange du ticket. Lire le corps
  via `zammad_get_ticket` seulement si le titre est trop vague pour résumer, et
  n'en tirer qu'un résumé métier — **jamais** de copier-coller du corps ni de
  données personnelles brutes (cf. règle de minimisation).

  ```markdown
  [https://covoiturage-betagouv.zammad.com/#ticket/zoom/<id>](https://covoiturage-betagouv.zammad.com/#ticket/zoom/<id>)
  <une phrase de contexte>
  ```

### 7. Confirmer

Afficher pour chaque tâche créée : libellé, `GEN-NNN` (renvoyé par
`notion-create-pages`), URL Notion. Récapituler l'état final (liés / créés /
laissés de côté).

## Pré-requis Notion (déjà en place)

Sur la database `b358a631afcc4e7a9a0a39092bf2d953`, provisionnés une fois :

1. Propriété **`zammad_url`** de type **URL** (clé de rapprochement).
2. Vue **« Tickets Zammad »** filtrée sur `zammad_url` non vide, triée par `ID`
   décroissant — rapprochement côté serveur, sans lire chaque page.

Si l'un des deux disparaît : recréer la propriété
(`ADD COLUMN "zammad_url" URL` via `notion-update-data-source`) et/ou la vue
(`notion-create-view`, filtre `zammad_url IS NOT EMPTY`), puis reporter le
nouveau `?v=…` dans les constantes. Tant que c'est absent, la synchro retombe
sur le repli de l'étape 3 (plus coûteux, moins fiable).

## Erreurs fréquentes

- **Rapprocher sur le `number`** au lieu de l'`id` interne → faux « manquants »,
  doublons. Toujours l'id du `/zoom/<id>`.
- **Filtrer les tâches sur `État ≠ Done`** pour le rapprochement → on rate les
  tickets dont la tâche a été clôturée et on recrée un doublon. Le rapprochement
  ignore l'`État`.
- **Créer sans demander** → action sortante non validée. Toujours l'étape 5.
- **`notion-search` pour collecter les tâches** → résultats incomplets. Utiliser
  la vue.

## Sortie

Rapport bref : nombre de tickets ouverts, nombre déjà liés, liste des manquants,
tâches créées (libellé + GEN-NNN + URL).
