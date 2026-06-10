---
name: point-equipe
description: Use when preparing the weekly standup ("point d'équipe", "stand-up hebdo", "visio hebdo", "prépare mon point équipe", "résume mes tâches de la semaine pour le standup"). Compiles tasks the current user closed this week (État Done + Date fermeture tâche), plus current blockers and next items, into a tight aide-mémoire formatted as numbered bullets (1.X / 2.X / 3.X) with GEN-* links, validates with the user, then writes a new row in the standup table.
allowed-tools: Bash, Skill, AskUserQuestion, mcp__claude_ai_Notion__notion-fetch, mcp__claude_ai_Notion__notion-search, mcp__claude_ai_Notion__notion-create-pages, mcp__claude_ai_Notion__notion-update-page
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
- Calculer la fenêtre **lundi → dimanche de la semaine en cours** (semaine ISO).
  Bash : `lundi=$(date -I -d "monday this week")` /
  `dimanche=$(date -I -d "sunday this week")`. Vérifier sur le calendrier que la
  fenêtre est cohérente (si on est un lundi très tôt, demander à l'utilisateur
  si on cible la semaine qui démarre ou celle qui finit).

### 2. Identifier les tâches Done de la semaine

But : lister les tâches avec **`État = Done`** + **`Date fermeture tâche` dans
[lundi, dimanche]** + **`Personne` = utilisateur courant**.

- L'API `notion-search` ne filtre pas sur les propriétés métier ; faire une
  recherche large sur la data source des tâches (mots-clés génériques :
  `Done terminée`, ou le nom de l'utilisateur), `page_size: 25`,
  `max_highlight_length: 0`.
- Lister les candidats récents (timestamps de la semaine), puis **fetch en
  parallèle** chaque candidat pour lire ses propriétés
  (`État`, `date:Date fermeture tâche:start`, `Personne`).
- Garder ceux qui matchent les trois critères. Ne **jamais** se fier au seul
  timestamp de la recherche : c'est la date de modification, pas de fermeture.

Si la recherche large rate des tâches (rare), demander à l'utilisateur s'il en
manque d'évidentes.

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
