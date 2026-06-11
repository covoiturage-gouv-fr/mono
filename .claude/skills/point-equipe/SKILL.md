---
name: point-equipe
description: Use when preparing the weekly standup ("point d'équipe", "stand-up hebdo", "visio hebdo", "prépare mon point équipe", "résume mes tâches de la semaine pour le standup"). Compiles tasks the current user closed this week (État Done + Date fermeture tâche), plus current blockers and next items, into a tight aide-mémoire formatted as numbered bullets (1.X / 2.X / 3.X) with GEN-* links, validates with the user, then writes a new row in the standup table.
allowed-tools: Bash, Skill, AskUserQuestion, mcp__plugin_Notion_notion__notion-fetch, mcp__plugin_Notion_notion__notion-search, mcp__plugin_Notion_notion__notion-query-database-view, mcp__plugin_Notion_notion__notion-create-pages, mcp__plugin_Notion_notion__notion-update-page
---

# Point d'équipe

Prépare la synthèse hebdo pour le stand-up. Le stand-up est court et mélange
tous les profils (PO, Coach, bizdev, tech), donc on reste au niveau « grandes
lignes » : un manager doit pouvoir survoler. Les détails se traitent en point
ad-hoc après le stand-up.

## Cibles Notion (constantes)

- **Suivi des tâches** (source des tâches Done) :
  `collection://2759b461-218e-4764-ae17-0025d728193c`
  - Vue **« Tâches Done »** (filtre `État = Done`, tri `Date fermeture`
    décroissante) — pour la section 1 :
    `https://www.notion.so/b358a631afcc4e7a9a0a39092bf2d953?v=272994bec93180eaac01000c57d33607`
  - Vue **« Liste Tâches / pers. Tech »** (filtre `État` To-do/In progress, tri
    `État` **décroissant** → les « Tâches priorisées » remontent en tête, avant
    le backlog) — pour bloquants et prochaines tâches :
    `https://www.notion.so/b358a631afcc4e7a9a0a39092bf2d953?v=28c994bec93180028480000cff33bc71`
  - Ces vues ne sont pas filtrées par personne : filtrer sur l'utilisateur
    courant en local. (Si elles sont renommées/supprimées, les retrouver via
    `notion-fetch` sur la database `b358a631afcc4e7a9a0a39092bf2d953`.)
- **Tableau hebdo « Semaine (modèle) »** (où on écrit) :
  `collection://35d994be-c931-800c-aca3-000be22465c3`
- **Page parent du stand-up** :
  <https://app.notion.com/p/356994bec9318053a8cacba279c7d066>
- Utilisateur courant : résoudre dynamiquement son ID Notion via `notion-search`
  (`query_type: "user"`, requête = l'email git de l'utilisateur, obtenu avec
  `git config user.email`). Ne jamais coder en dur d'identité (nom, ID, email).

Schéma de la table hebdo (vérifier via `notion-fetch` sur la data source si
besoin) :

| Colonne | Type | Contenu |
| --- | --- | --- |
| `Membre` | title | Nom du membre (celui de l'utilisateur courant) |
| `Stand up` | date | Date du stand-up (jour de la visio) |
| `Points saillants réalisés` | text | Section 1 |
| `Points bloquants` | text | Section 2 |
| `Prochaines tâches` | text | Section 3 |

## Règles transverses

- **Aide-mémoire, pas rapport** : chaque puce = quelques mots, pas une phrase.
  Le but est de servir d'antisèche pendant la visio.
- **Pas de détail technique, pas de TODO technique** (« merger », « déployer »,
  « renommer X ») : que les grandes lignes pertinentes pour tous les profils.
- **Numérotation `section.point`** sur chaque puce (`1.1`, `1.2`, … `2.1`, …,
  `3.1`, …) pour que l'utilisateur puisse pointer une ligne à corriger.
- **Lien GEN-*** en fin de chaque puce, sous la forme `— [GEN-NNN](url)`. Sans
  ticket associé, mettre juste le libellé.
- **Français correct** (invoquer le skill `french` avant de rédiger), accents
  sur majuscules, anglicismes métier admis (release, merge, scope, backfill...).
- **Undercover** : aucune mention de Claude / IA / assistant.
- **Vues plutôt que pages** : pour collecter des tâches, toujours passer par une
  requête de vue (`notion-query-database-view`) — filtres/tris appliqués côté
  serveur, propriétés renvoyées sans le corps des pages. Ne `fetch` une page
  (corps complet, coûteux) qu'en dernier recours, pour lire un contenu qu'un
  titre ne permet pas de reformuler. Éviter `notion-search` pour les sélections
  de tâches (classe par modification/pertinence, rate des résultats).
- **Action sortante** : Notion est externe. Toujours montrer le contenu et
  **confirmer avec l'utilisateur avant d'écrire**.

## Procédure

### 1. Préparer

- Invoquer le skill `french`.
- Calculer la date du jour et la fenêtre **lundi → dimanche de la semaine ISO**
  par décalage sur le jour de semaine (`%u`). Ne **pas** utiliser l'idiome
  `… this week` : selon la locale il renvoie un lundi *postérieur* au dimanche
  (vu un jeudi : `monday this week` = lundi suivant), d'où une fenêtre incohérente.

  ```bash
  jour=$(date -I)
  dow=$(date +%u)                               # 1=lundi … 7=dimanche
  lundi=$(date -I -d "$jour -$((dow - 1)) days")
  dimanche=$(date -I -d "$jour +$((7 - dow)) days")
  ```

  Correct quel que soit le jour de préparation, bords d'année compris. Si on
  prépare un lundi très tôt pour la semaine précédente, demander à l'utilisateur
  quelle semaine cibler.

### 2. Identifier les tâches Done de la semaine

But : lister les tâches avec **`État = Done`** + **`Date fermeture tâche` dans
[lundi, dimanche]** + **`Personne` = utilisateur courant**.

Méthode : interroger la vue **« Tâches Done »** avec
`notion-query-database-view` (URL dans les constantes). Elle filtre déjà
`État = Done` côté serveur et trie par `Date fermeture tâche` **décroissante**,
donc les fermetures les plus récentes arrivent en tête.

- La réponse renvoie directement les propriétés **sans le corps des pages** :
  `date:Date fermeture tâche:start`, `Personne` (tableau d'`user://…`),
  `userDefined:ID` (= le numéro `GEN-NNN`), `Tâche`, `url`. Pas de `fetch` page
  par page.
- Filtrer en local : `Personne` contient l'utilisateur courant **et**
  `date:Date fermeture tâche:start` ∈ [lundi, dimanche]. Comme c'est trié par
  date décroissante, s'arrêter dès qu'on passe sous `lundi` ; ne paginer
  (`start_cursor` = `next_cursor`) que si la première page n'a pas encore
  atteint `lundi`.
- Ne lire le corps d'une tâche (`notion-fetch`) que si son libellé est trop
  vague pour rédiger la puce.

Ne **pas** utiliser `notion-search` ici : il classe par date de *modification*
et par pertinence sémantique, rate des tâches (ex. observé : une tâche fermée
dans la semaine jamais remontée) et oblige à lire chaque page en entier. Le
garder uniquement en filet de secours si la vue « Tâches Done » est
indisponible.

### 3. Identifier bloquants et prochaines tâches

Ces deux sections ne se déduisent pas des Done : elles viennent de la
conversation, des PR ouvertes, et des tâches **en cours / priorisées** du suivi.
Pour ces dernières, interroger la vue **« Liste Tâches / pers. Tech »**
(`notion-query-database-view`, URL dans les constantes) et filtrer sur
l'utilisateur courant — même logique que l'étape 2. Comme elle est triée
priorisées d'abord, lire le haut de la liste suffit : ignorer les tâches
récurrentes « Run mensuel » et arrêter dès qu'on atteint le backlog (`État`
repasse à `Backlog`).

- **Bloquants** : tâches en attente d'une décision externe (équipe, partenaire,
  donneur d'ordre), ou correctifs livrés dont l'exécution finale attend un
  arbitrage. À tirer : du suivi Notion (`État = Tâches priorisées` + propriété
  Personne) et des derniers débriefs.
- **Prochaines tâches** : sujets que l'utilisateur compte attaquer la semaine
  d'après - tâches priorisées non démarrées, suites logiques des Done de la
  semaine. Pas la liste des TODO techniques internes.

En cas de doute sur le périmètre, demander à l'utilisateur via
`AskUserQuestion` (ex. « j'inclus la tâche X en bloquants ou en prochaines ? »).

### 4. Rédiger la synthèse

Format de présentation à l'utilisateur (Markdown, dans la conversation) :

```markdown
### 1. Points saillants réalisés
- 1.1 <libellé court> — [GEN-NNN](url)
- 1.2 ...

### 2. Points bloquants / d'attention
- 2.1 <libellé court> — [GEN-NNN](url)

### 3. Prochaines tâches
- 3.1 <libellé court> — [GEN-NNN](url)
- 3.2 ...
```

Règles d'écriture :

- Une puce = quelques mots, pas une phrase complète. Ex. : « Territoire SIOCA
  créé en prod (Ouest Cornouaille, 4 EPCI) ».
- Pas de verbe d'action technique (« merger », « déployer », « réindexer ») :
  reformuler en sujet. Ex. : pas « Merger les PR du décalage de date » mais
  « Correction décalage de date export espace partenaire ».
- Pas d'URL de PR GitHub dans les puces : seul le lien GEN-* à la fin.
- Section vide -> « _RAS_ », ne pas la supprimer.

### 5. Confirmer

Montrer la synthèse à l'utilisateur et attendre validation **avant d'écrire**.
Annoncer explicitement la cible (« je vais ajouter une ligne dans la table
"Semaine (modèle)" de la page Point d'équipe, datée du <jour de stand-up> »).
Si l'utilisateur demande des corrections, il pointera la puce par sa
numérotation (« corrige 2.1 »).

Demander aussi la **date du stand-up** si elle n'est pas le jour courant
(souvent le mardi ou mercredi, pas forcément le jour où on prépare).

### 6. Écrire dans le tableau hebdo

`notion-create-pages` :

- `parent` : `{ "type": "data_source_id", "data_source_id": "35d994be-c931-800c-aca3-000be22465c3" }`
- `content` : laisser vide (la valeur passe dans les propriétés, pas le corps).
- `properties` :
  - `Membre` : nom de l'utilisateur courant (titre).
  - `date:Stand up:start` : date du stand-up (`AAAA-MM-JJ`).
  - `date:Stand up:is_datetime` : `0`.
  - `Points saillants réalisés` : le bloc Markdown de la section 1 (sans titre,
    seulement les puces `1.1 …`, `1.2 …`).
  - `Points bloquants` : bloc de la section 2.
  - `Prochaines tâches` : bloc de la section 3.

Notion stocke ces champs en texte brut riche : les `[GEN-NNN](url)` y restent
sous forme de lien cliquable.

### 7. Renvoyer la confirmation

Afficher : ligne ajoutée, date du stand-up, et l'URL de la nouvelle entrée
(retournée par `notion-create-pages`). Rappeler à l'utilisateur qu'il peut
encore éditer la ligne directement dans Notion si besoin.

## Sortie

Rapport bref : nombre de puces par section, date du stand-up, URL de la ligne
créée.
