---
name: point-equipe
description: Use when preparing the weekly standup ("point d'équipe", "stand-up hebdo", "visio hebdo", "prépare mon point équipe", "résume mes tâches de la semaine pour le standup"). Compiles tasks the current user closed this week (État Done + Date fermeture tâche) plus the user's PRs merged this week on mono (merge date = done, catches work with no Notion task; infra work is tracked as Notion "Chantier Infra" tasks), plus current blockers and next items, into a tight aide-mémoire formatted as numbered bullets (1.X / 2.X / 3.X) with GEN-* links, validates with the user, then writes a new row in the standup table.
allowed-tools: Bash, Skill, AskUserQuestion, mcp__github__search_pull_requests, mcp__github__get_me, mcp__claude_ai_Notion__notion-query-data-sources, mcp__claude_ai_Notion__notion-fetch, mcp__claude_ai_Notion__notion-search, mcp__claude_ai_Notion__notion-create-pages, mcp__claude_ai_Notion__notion-update-page
---

# Point d'équipe

Prépare la synthèse hebdo pour le stand-up. Le stand-up est court et mélange
tous les profils (PO, Coach, bizdev, tech), donc on reste au niveau « grandes
lignes » : un manager doit pouvoir survoler. Les détails se traitent en point
ad-hoc après le stand-up.

## Cibles Notion (constantes)

- **Suivi des tâches** (source des tâches Done) :
  `collection://2759b461-218e-4764-ae17-0025d728193c`
- **Tableau hebdo « Semaine (modèle) »** (où on écrit) :
  `collection://35d994be-c931-800c-aca3-000be22465c3`
- **Page parent du stand-up** :
  <https://app.notion.com/p/356994bec9318053a8cacba279c7d066>
- **Dépôt GitHub** (source des PR mergées) : `covoiturage-gouv-fr/mono`
  uniquement. Le dépôt `infra` est privé et hors d'atteinte : le travail infra
  est suivi dans Notion par les tâches rattachées au projet **Chantier Infra**
  (relation `🛣️ Roadmap Projets 2026` →
  `https://app.notion.com/3d6994bec931815cbe02ca4039048bea`). Ne pas interroger
  GitHub pour l'infra.
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
- **Action sortante** : Notion est externe. Toujours montrer le contenu et
  **confirmer avec l'utilisateur avant d'écrire**.

## Procédure

### 1. Préparer

- Invoquer le skill `french`.
- Récupérer la date du jour : `date -I`.
- **Le stand-up a lieu tous les jeudis.** La date du stand-up (`Stand up`) est le
  jeudi de la semaine — le jeudi courant si on prépare un jeudi, sinon le jeudi le
  plus proche (demander en cas d'ambiguïté).
- Calculer la fenêtre **jeudi précédent → jeudi du stand-up, inclus** (7 jours
  glissants). Bash : `fin=<date du jeudi du stand-up>` puis
  `debut=$(date -I -d "$fin -7 days")`. Ne pas se fier à `date -d "... this week"`
  (le calcul GNU du lundi/jeudi « this week » est ambigu selon le jour courant) :
  vérifier la fenêtre sur le calendrier.

### 2. Identifier les tâches Done de la semaine

But : lister les tâches avec **`État = Done`** + **`Date fermeture tâche` dans
[jeudi précédent, jeudi du stand-up]** + **`Personne` = utilisateur courant**.

Interroger la data source en SQL avec `notion-query-data-sources` (les noms de
colonnes utiles : `Tâche`, `État`, `Personne`, `userDefined:ID` (le numéro
GEN-*), `date:Date fermeture tâche:start`, `🛣️ Roadmap Projets 2026`, `url`) :

```sql
SELECT "userDefined:ID" AS id, "Tâche" AS titre,
       "date:Date fermeture tâche:start" AS fermeture, url
FROM "collection://2759b461-218e-4764-ae17-0025d728193c"
WHERE "État" = 'Done'
  AND "Personne" LIKE '%<id utilisateur>%'
  AND date("date:Date fermeture tâche:start") >= '<debut>'
ORDER BY fermeture DESC LIMIT 100
```

- **Piège : le résultat est tronqué à 25 lignes sans le signaler**
  (`has_more: false` ment). Toujours `ORDER BY fermeture DESC` + `LIMIT` explicite,
  et si le compte atteint la troncature, re-découper la fenêtre par tranches de
  dates. Un tri ascendant ferait disparaître les tâches les plus récentes.
- Ne **jamais** se fier au timestamp de recherche : c'est la date de
  modification, pas de fermeture.

Si des tâches évidentes manquent, demander à l'utilisateur.

### 2 bis. Compléter avec les PR mergées de la semaine

Beaucoup de travail applicatif n'a pas de tâche Notion. On rattrape via les PR
mergées sur `mono` (le travail infra, lui, a toujours sa tâche Notion sous
**Chantier Infra** — voir étape 2 —, donc rien à chercher côté GitHub).

- Chercher les PR de l'utilisateur mergées dans la fenêtre avec
  `mcp__github__search_pull_requests` :
  `repo:covoiturage-gouv-fr/mono is:merged author:<login> merged:<debut>..<fin>`
  (`fields: ["number", "title", "html_url", "closed_at"]`).
  Le login GitHub se résout dynamiquement (`mcp__github__get_me`) : ne jamais le
  coder en dur.
- **La date de merge fait foi comme date de « done »** (`closed_at` sur une PR
  mergée). C'est l'équivalent de `Date fermeture tâche` pour le travail sans
  ticket.
- **Dédoublonner** : une PR déjà couverte par une tâche Done de l'étape 2 (même
  sujet, ticket GEN-* cité dans le titre ou le corps de la PR) ne donne pas de
  puce en plus ; elle vient juste confirmer la tâche.
- Les PR restantes (sans tâche Notion) deviennent des puces de la section 1,
  reformulées en sujet métier, sans numéro de PR ni URL GitHub, et sans lien
  GEN-* puisqu'il n'y en a pas.
- En cas de doute sur une PR (travail mineur, chore CI, dépendances), demander à
  l'utilisateur via `AskUserQuestion` s'il la garde.

### 3. Identifier bloquants et prochaines tâches

Ces deux sections ne se déduisent pas des Done : elles viennent de la
conversation, des PR ouvertes, et des tâches **en cours / priorisées** du suivi.

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

La **date du stand-up** est le **jeudi** de la semaine. Si on prépare un jeudi,
c'est le jour courant ; sinon, prendre le jeudi le plus proche et confirmer avec
l'utilisateur.

### 6. Écrire dans le tableau hebdo

`notion-create-pages` :

- `parent` : `{ "type": "data_source_id", "data_source_id": "35d994be-c931-800c-aca3-000be22465c3" }`
- `content` : laisser vide (la valeur passe dans les propriétés, pas le corps).
- `properties` :
  - `Membre` : nom de l'utilisateur courant (titre).
  - `date:Stand up:start` : date du stand-up (`AAAA-MM-JJ`).
  - `date:Stand up:is_datetime` : `0` en **nombre**, pas la chaîne `"0"`
    (l'API rejette la chaîne avec une erreur 400).
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
créée, et nombre de PR mergées retenues sans tâche Notion associée.
