---
name: good-morning
description: Use for the morning briefing / start-of-day routine ("good morning", "bonjour", "brief du matin", "ma matinée", "what's on today", "prépare ma journée"). Syncs the current user's open Zammad tickets to Notion (via zammad-sync), summarises and prioritises those tickets, triages the unassigned Zammad queue and flags the ones that could be assigned to the user, then lists the user's top Notion tasks by priority.
allowed-tools: Bash, Skill, AskUserQuestion, mcp__zammad__zammad_get_current_user, mcp__zammad__zammad_search_tickets, mcp__zammad__zammad_get_ticket, mcp__claude_ai_Notion__notion-query-database-view, mcp__claude_ai_Notion__notion-search
---

# Brief du matin

Routine de démarrage de journée. Quatre volets, dans l'ordre :

1. **Synchro** des tickets Zammad ouverts vers Notion (skill `zammad-sync`).
2. **Résumé priorisé** de mes tickets Zammad ouverts.
3. **Tri de la file non assignée** + tickets qui pourraient m'être attribués.
4. **Top 5** de mes tâches Notion par priorité.

Lecture d'abord, action ensuite. Le seul volet qui écrit (synchro) passe par
`zammad-sync`, qui demande validation avant de créer quoi que ce soit.

## Identités (constantes locales)

Stables en local — **ne pas re-résoudre** à chaque matin (cf. mémoire
`identites-jonathan`) :

- **Zammad** : login `jonathan.fallon@beta.gouv.fr`, id `3`, groupe `Technique`.
- **Notion** : `user://cea451b0-d0ea-48ed-9fb0-240a95de8739`.
- **git** : `jonathan@scopopop.com`.

Repli si une de ces valeurs paraît fausse : `zammad_get_current_user` (→ `login`)
et `git config user.email` → `notion-search` (`query_type: "user"`). Si ça
diffère, corriger la mémoire `identites-jonathan`.

## Cibles Notion (constantes)

- Database **Suivi des tâches** : `b358a631afcc4e7a9a0a39092bf2d953`
  (data source `collection://2759b461-218e-4764-ae17-0025d728193c`).
- Vue **« Liste Tâches / pers. Tech »** (États actifs, tri par État) :
  `https://www.notion.so/b358a631afcc4e7a9a0a39092bf2d953?v=28c994bec93180028480000cff33bc71`
  — non filtrée par personne ; filtrer localement sur l'identité Notion.

## Heuristique de priorité (volets 2 et 3)

La priorité Zammad native est presque toujours `2 normal` → non discriminante.
Classer / flaguer « pour moi » selon ces signaux (du + fort au + faible) :

- **Sécurité / incident technique** → +++ (homologation, faille, panne, données).
- **Opérateur ou territoire (AOM, collectivité, région)** comme client → ++.
  L'opérateur/territoire se déduit du domaine email ou de l'organisation Zammad.
- **Adressé à `technique@`** (groupe `Technique`) → ++.
- **Balle dans mon camp** : le client a répondu *après* ma dernière réponse
  (`last_contact_customer_at` > `last_contact_agent_at`) → à traiter.
- **Échéance de MAJ dépassée / proche** (`update_escalation_at` passé) → remonter.
- **Ancienneté** : à signaux égaux, le plus ancien non résolu d'abord.
- **Hors périmètre** : bizdev, webinaires, démarchage commercial, questions
  d'usagers grand public (emails génériques type gmail) → **pas pour moi**,
  ne pas remonter (ou marquer « support N1 »).

## Procédure

### Volet 1 — Synchro Zammad → Notion

Invoquer le skill **`zammad-sync`** (`Skill` tool). Il liste mes tickets ouverts,
les rapproche du suivi Notion via `zammad_url`, montre les manquants et **attend
ma validation** avant de créer. Reporter son verdict en une ligne (créés / déjà
liés / laissés). Ne pas ré-implémenter la synchro ici.

### Volet 2 — Mes tickets Zammad ouverts, priorisés

`zammad_search_tickets` : `owner = jonathan.fallon@beta.gouv.fr`, `state = open`,
`per_page = 100`, `response_format = json`. Pour chaque ticket retenir : `id`,
`number`, `title`, `customer`, `organization`, `group`, `last_contact_customer_at`,
`last_contact_agent_at`, `update_escalation_at`, `created_at`.

Classer avec l'heuristique ci-dessus. Tableau :

```markdown
| Prio | Ticket | Sujet | Notion | Pourquoi |
| --- | --- | --- | --- | --- |
```

- `Ticket` = lien `…/#ticket/zoom/<id>` (id interne) sur le `number`.
- `Notion` = le `GEN-NNN` lié (réutiliser le rapprochement du volet 1) ou `—`.
- `Pourquoi` = le signal qui justifie le rang (1 phrase courte).

### Volet 3 — File non assignée + ce qui peut m'être attribué

`zammad_search_tickets` : `query = "owner_id:1 AND (state.name:open OR state.name:new)"`,
`per_page = 100`, `response_format = json`. (`owner_id:1` = non assigné dans Zammad.)

Pour chacun, décider « pour moi ? » avec l'heuristique. Tableau :

```markdown
| Ticket | Sujet | Groupe | Pour moi ? |
| --- | --- | --- | --- |
```

- ⭐ **Oui** si opérateur/territoire/sécurité/technique → expliquer en 3 mots.
- ❌ **Non** si bizdev / webinaire / usager grand public → dire pourquoi.

Ne **pas** réassigner automatiquement (action sortante). Juste pointer les
candidats ; proposer en fin de brief de m'en attribuer un si je le demande.

### Volet 4 — Top 5 tâches Notion

Interroger la vue **« Liste Tâches / pers. Tech »**
(`notion-query-database-view`). Filtrer localement :

- `Personne` contient `user://cea451b0-d0ea-48ed-9fb0-240a95de8739`,
- `État` = `Tâches priorisées` (exclure `Backlog` et `Done`).

Trier par `Priority` (`🔥 High`/`🔥🔥 Run` > `⏰ Medium` > `🧘 Low` > sans prio)
et garder les **5 premières**. La vue trie par État (les `Tâches priorisées`
remontent), donc les tâches à plus forte priorité sont en tête de page 1 ;
paginer (`start_cursor`/`next_cursor`) **dans la même session de requête** tant
que `has_more` si besoin de remonter au-delà. Un `next_cursor` issu d'une
requête antérieure expire (`Invalid or expired pagination cursor`) — toujours
repartir d'une requête fraîche, ne jamais réutiliser un curseur d'avant.

```markdown
**Top 5 tâches**
- GEN-567 — Export SYTRAL 🔥🔥 Run
- …
```

`GEN-NNN` = propriété `userDefined:ID`. Lien = `url` de la tâche.

### Sortie

Un brief unique, titré `☀️ Good morning <prénom> — <date>`, avec les 4 sections
dans l'ordre. Concis : tableaux pour 2 et 3, liste pour 4. Finir par une ligne
d'appel à l'action (« je t'attribue le 347682 ? je traite le Karos ? »).

## Erreurs fréquentes

- **Re-résoudre les identités** chaque matin → inutile, elles sont en constantes.
- **Réassigner / créer sans validation** → la synchro passe par `zammad-sync`
  (qui valide) ; l'attribution d'un ticket non assigné se fait sur demande.
- **Couper le Top 5 avant de paginer** la vue Notion → on rate des tâches.
- **Remonter du bizdev / des webinaires / des usagers grand public** comme « pour
  moi » → hors périmètre technique.
- **Trier sur la priorité Zammad native** (`2 normal` partout) → non
  discriminante ; utiliser l'heuristique métier.
