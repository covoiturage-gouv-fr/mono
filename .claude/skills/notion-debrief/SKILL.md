---
name: notion-debrief
description: Use when documenting/journaling finished work into the Notion "Suivi des tâches Data" board - triggers on "documente la tâche dans Notion", "post task", "débrief Notion", "note le travail dans le suivi des tâches", "journalise dans Notion", or any request to record what was done after finishing a chantier. Reuses an existing task or creates one, writes a French debrief (contexte, travail réalisé, ressources liées, hors-scope, prochaines étapes), and returns the Notion URL.
allowed-tools: Read, Bash, Grep, Glob, Skill, mcp__claude_ai_Notion__notion-fetch, mcp__claude_ai_Notion__notion-search, mcp__claude_ai_Notion__notion-create-pages, mcp__claude_ai_Notion__notion-update-page, mcp__claude_ai_Notion__notion-query-database-view, mcp__github__search_pull_requests, mcp__github__pull_request_read
---

# Notion Debrief

Documente le travail livré dans le suivi Notion de l'équipe Data, pour garder un
historique fouillable plus tard (bugs, évolutions, décisions). Réutilise une tâche
existante ou en crée une, rédige un débrief en français clair, puis renvoie l'URL.

## But

Construire, tâche après tâche, une mémoire détaillée des changements de l'app RPC.
Chaque débrief doit pouvoir être relu dans six mois pour comprendre *pourquoi* et
*comment* un changement a été fait. Privilégier l'utilité de l'historique sur la
brièveté : mieux vaut un contexte explicite qu'une note cryptique.

## Cible Notion (constantes)

- Data source (parent des tâches) : `collection://2759b461-218e-4764-ae17-0025d728193c`
- Propriété titre : `Tâche`
- Statut `État` (type status) -> **`Tâches priorisées`** par défaut.
  Ne jamais écrire `Done` : l'utilisateur passe la tâche en `Done` après relecture.
- Assignee `Personne` (person, tableau JSON d'IDs) -> par défaut l'utilisateur courant.
  - Jonathan Fallon = `cea451b0-d0ea-48ed-9fb0-240a95de8739` (jonathan@scopopop.com).
  - Si quelqu'un d'autre lance le skill, résoudre son ID via `notion-search`
    (`query_type: "user"`, requête = son email).

Si le schéma a changé (propriété renommée, nouvel `État`), faire un `notion-fetch`
sur la data source pour relire les noms exacts avant d'écrire.

## Règles transverses

- **Undercover** : aucune mention de Claude, d'IA ou d'assistant dans le titre ou le
  contenu Notion. On écrit à la première personne de l'équipe.
- **Langage** : français correct (invoquer le skill `french`), simple et direct.
  Pas de mode caveman dans le contenu Notion.
- **Historique** : en réutilisation, on **ajoute** une section datée en fin de page.
  On n'écrase jamais le contenu existant - l'historique doit s'accumuler.
- **Action sortante** : Notion est un service externe. Toujours montrer le contenu et
  **confirmer avec l'utilisateur avant d'écrire**.

## Procédure

### 1. Préparer
- Invoquer le skill `french`.
- Récupérer la date du jour : `date -I` (sert à dater les entrées d'historique).

### 2. Rassembler le contexte
Réunir la matière du débrief depuis :
- la conversation courante (ce qui a été fait, décisions, points laissés de côté) ;
- git : `git branch --show-current`, `git log --oneline -15`, et la PR liée -
  `mcp__github__search_pull_requests` sur la branche (sinon `gh pr view --json url,title,number`) ;
- les liens fournis par l'utilisateur : PR GitHub, tickets, tickets Zammad, autres
  tâches Notion. Demander s'il en manque un évident.

### 3. Réutiliser ou créer
- Chercher une tâche existante liée au sujet via `notion-search`
  (`data_source_url` = la data source ci-dessus, requête = mots-clés du sujet).
- **Match clair** -> réutiliser la tâche (étape 6, mise à jour).
- **Ambigu** (plusieurs candidates, ou correspondance faible) -> demander à
  l'utilisateur via `AskUserQuestion` : réutiliser laquelle, ou créer une nouvelle.
- **Aucun match** -> créer une nouvelle tâche (étape 6, création).

### 4. Rédiger le débrief
Contenu en Markdown Notion, français, avec **ce gabarit exact** (garder ces 5
titres ; laisser une section vide avec "_RAS_" plutôt que la supprimer) :

```markdown
## Contexte
Pourquoi ce travail : besoin, problème, demande à l'origine, résultat visé.

## Travail réalisé
Les changements qui comptent pour le métier, la tech ou l'infra (voir "Niveau de
détail" ci-dessous). Synthèse, pas journal d'opérations.

## Ressources liées
- PR : [#1234 - titre](url)
- Ticket : [ref](url)
- Zammad : [#ticket](url)
- Notion : [tâche liée](url)

## Hors-scope / remarques
Ce qui a été volontairement écarté, limites connues, dette laissée, pièges.

## Prochaines étapes
- [ ] étape suivante
```

**Niveau de détail de "Travail réalisé"**

Objectif : qu'une relecture dans six mois comprenne *ce qui a changé pour le produit
et le système*, pas comment on a manipulé l'outillage.

- **Inclure** : changements de comportement métier, d'architecture, de schéma de
  données, de configuration CI/infra, de contrats d'API, de règles ; impacts et
  effets de bord ; décisions structurantes.
- **Exclure** : la mécanique technique sans valeur de relecture - opérations git
  (commit, branche, worktree, merge, squash, tags), nettoyage d'environnement local,
  exécutions de routine (lint, fmt, install), allers-retours d'outillage.

La fusion d'une PR ou un changement d'état peut être mentionné en une ligne s'il
porte un fait utile (ex. "déployé en prod", "release coupée/non coupée"), mais ce
n'est jamais le sujet de la section.

Exemple (cas réel PR #3211, à resserrer) :

- Trop verbeux : "Fusion confirmée sur `main` (squash `1368c4709`) ; worktree local
  supprimé ; branche `worktree-chore+release-hardening` supprimée en local et côté
  distant ; 12 checks requis confirmés."
- Resserré : "Périmétrage du versionnage validé en conditions réelles : un merge
  CI-only n'a pas coupé de release applicative (tag inchangé `v3.97.2`)."

### 5. Confirmer
Montrer à l'utilisateur le titre, les propriétés (Personne, État) et le contenu.
Indiquer s'il s'agit d'une création ou d'une mise à jour, et de quelle tâche.
Attendre son accord avant d'écrire.

### 6. Écrire dans Notion

**Création** (`notion-create-pages`) :
- parent : `{ "type": "data_source_id", "data_source_id": "2759b461-218e-4764-ae17-0025d728193c" }`
- `content` : le débrief de l'étape 4 (sans répéter le titre en tête).
- `properties` :
  - `Tâche` : titre court et parlant (FR, ASCII des refs techniques toléré) ;
  - `Personne` : `["cea451b0-d0ea-48ed-9fb0-240a95de8739"]` (ou l'ID résolu) ;
  - `État` : `Tâches priorisées`.

**Réutilisation** (`notion-update-page`) :
- `command: "insert_content"`, `position: { "type": "end" }`.
- `content` : un séparateur puis une section datée, pour empiler l'historique :

  ```markdown
  ---
  ### Mise à jour - AAAA-MM-JJ
  ```
  suivie du même gabarit (Contexte / Travail réalisé / Ressources liées /
  Hors-scope / Prochaines étapes) pour cette itération.
- Mettre à jour `Personne`/`État` (`command: "update_properties"`) seulement si
  nécessaire ; ne jamais repasser en `Done`.

### 7. Renvoyer l'URL
Toujours afficher à l'utilisateur l'URL Notion de la tâche créée ou mise à jour,
et rappeler qu'il la passe en `Done` après relecture.

## Sortie

Rapport bref : création ou mise à jour, titre de la tâche, propriétés posées
(Personne, État), et l'URL Notion cliquable.
