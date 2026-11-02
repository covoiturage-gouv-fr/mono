---
name: livrable-malt
description: Use when compiling the team's monthly deliverables ("livrable Malt", "livrables du mois", "compile les CRA de l'équipe", "livrable de juillet") from all the CRA of a billed month in the "Suivi Malt ISN 2026" Notion page, for a document classified NON PROTÉGÉ / PUBLIC.
allowed-tools: Bash, Skill, AskUserQuestion, mcp__claude_ai_Notion__notion-fetch, mcp__claude_ai_Notion__notion-search, mcp__claude_ai_Notion__notion-query-data-sources, mcp__claude_ai_Notion__notion-create-pages, mcp__claude_ai_Notion__notion-update-page
---

# Livrable Malt

Compile les CRA de **toute l'équipe** pour un mois facturé en une liste unique
de livrables, recoupée avec les tâches Notion et les PR. Pendant collectif du
skill `cra-mensuel` (CRA individuel).

## Sources

| Source | Référence |
| --- | --- |
| Base **CRA** (entrée) | `collection://0ecfd554-798d-460b-ac27-d2b30374d40f` |
| Base **Livrables** (sortie) | `collection://1e9eba5b-bb0f-4faf-805f-35a667b28a0b` |
| Page parente | <https://app.notion.com/p/3b3994bec93180fc97bed6459839770d> |
| Suivi des tâches | `collection://2759b461-218e-4764-ae17-0025d728193c` |
| PR fusionnées | `git log origin/main origin/next` (pas `gh`) |

## Classification : NON PROTÉGÉ — PUBLIC

Le livrable ne contient que des informations dont la divulgation n'aurait aucun
effet préjudiciable. Chaque puce passe ce filtre :

- **Pas de données personnelles** : aucun nom de membre de l'équipe ni
  d'interlocuteur.
- **Noms d'organismes publics admis** (collectivités, AOM, régions, SCoT,
  territoires) : ils situent le travail.
- **Pas d'entreprise privée** (opérateurs de covoiturage) ni de montant, de
  volume de trajets ou d'information commerciale.
- **Pas de détail technique sensible** : ni noms d'outils internes, de schémas,
  d'index, de tables, de jetons, de codes HTTP, ni description de faille ou
  d'architecture. Décrire l'effet : « Amélioration de la gestion des erreurs
  d'authentification », pas « 401 au lieu de 500 sur jeton invalide ».
- **Undercover** : aucune mention d'IA ou d'assistant ; l'outillage interne se
  décrit par son effet.

## Procédure

### 1. Mois facturé

Le livrable porte le mois de la propriété `Mois` des CRA. Annoncer le mois et
la fenêtre (`AAAA-MM-01 → AAAA-MM-JJ`).

### 2. Lire tous les CRA du mois

```sql
SELECT Titre, "date:Mois:start" AS mois, Personne, Statut, url
FROM "collection://0ecfd554-798d-460b-ac27-d2b30374d40f"
WHERE "date:Mois:start" = '<AAAA-MM-01>'
```

Puis `notion-fetch` sur chaque `url`. Signaler les CRA encore en `Brouillon` et
les membres sans CRA ce mois-ci (comparer aux `Personne` du mois précédent).

### 3. Recouper

Fenêtre = le mois facturé. Lire `main` **et** `next` : depuis le gitflow, une PR
peut n'être fusionnée que dans `next`.

```bash
git fetch -q origin main next
git log origin/main origin/next --since=<debut> --until=<fin+1j> \
  --date=short --pretty='%cd | %an | %s' | grep -E '\(#[0-9]+\)' | sort -u
```

Le CRA fait foi : une PR qu'il cite reste dans le livrable, même si elle a été
fusionnée dans les derniers jours du mois précédent.

- **PR manquantes** : pour une puce sans PR, chercher la tâche Notion
  correspondante (État `Done`, fermée dans le mois, `Personne` contient
  l'auteur) et la PR qui la livre, même si un autre membre l'a fusionnée.
- **Orphelins** : lister sous le bloc les PR du mois absentes de tout CRA,
  sans les ajouter d'office.

### 4. Rédiger

Invoquer le skill `french` avant de rédiger.

```
Livrables - <Mois AAAA>

PM / Déploiement :
- puce

Développement / Data :
- puce (PR #3221, #3217)

Sécurité / Infra :
- puce (PR #3211)
```

- **Trois thèmes fixes, dans cet ordre.** PM / Déploiement : collectivités,
  territoires, appels de fonds (production, vérification, publication), club,
  démos, communication, support. Développement / Data : exports, plateforme de
  données, modèles, observatoire, espace partenaire, fonctionnalités.
  Sécurité / Infra : dépendances, authentification, CI, release, images,
  environnement de développement, documentation de contribution, outillage
  interne de l'équipe.
- **Puces nominales**, un fait par puce, sans nom de personne.
- **Fusionner les doublons** entre CRA (même accompagnement, même vérification
  des appels de fonds…), en réunissant les organismes cités entre parenthèses.
- **PR en fin de puce**, numéros seuls, comme dans les CRA. Une puce sans PR
  reste sans PR, jamais de numéro inventé.
- Écrire le bloc dans le scratchpad.

Sous le bloc : CRA sources (et leur statut), PR ajoutées par recoupement, PR
orphelines, détails retirés pour la classification.

### 5. Publier (sur demande)

Annoncer la cible avant d'écrire. Une ligne dans la base Livrables via
`notion-create-pages`, parent `data_source_id: 1e9eba5b-bb0f-4faf-805f-35a667b28a0b` :

| Propriété | Valeur |
| --- | --- |
| `Titre` | `Livrables - <Mois AAAA>` |
| `date:Mois:start` | `AAAA-MM-01` |
| `Statut` | `Brouillon` (puis `Envoyé`, `Validé`) |
| `CRA` | URL des CRA sources |

Contenu = le bloc sans sa ligne de titre : thèmes en paragraphe suivi de `:`,
puces en liste à tirets. Si la ligne du mois existe déjà, la mettre à jour avec
`notion-update-page` plutôt que d'en créer une seconde. Renvoyer l'URL.
