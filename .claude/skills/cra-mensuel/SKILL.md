---
name: cra-mensuel
description: Use when producing the monthly activity report / CRA ("CRA mensuel", "fais mon CRA", "compte rendu d'activité", "CRA de juillet", "rapport d'activité du mois"). Lists the tasks the current user closed in the target month (Notion "Suivi des tâches", État Done + Date fermeture tâche), cross-checks them against merged PRs, numbers everything so the user can pick, then renders a plain-text French report grouped by theme, capped at 5000 characters.
allowed-tools: Bash, Skill, AskUserQuestion, mcp__claude_ai_Notion__notion-fetch, mcp__claude_ai_Notion__notion-search, mcp__claude_ai_Notion__notion-query-data-sources
---

# CRA mensuel

Produit le compte rendu d'activité du mois : les travaux menés par l'utilisateur,
groupés par thème, prêts à coller dans un mail ou un formulaire.

Le CRA est un **livrable sortant** (facturation, suivi de mission) : la sélection
des tâches appartient à l'utilisateur, jamais au skill.

## Sources

- **Notion, « Suivi des tâches »** : `collection://2759b461-218e-4764-ae17-0025d728193c`.
  Propriétés utiles : `Tâche` (titre), `État` (status), `Date fermeture tâche`,
  `Personne`, `userDefined:ID` (référence GEN-*).
- **Historique git local** : les PR fusionnées, pour rattraper le travail sans
  carte Notion.
- Utilisateur courant : résoudre son ID Notion via `notion-search`
  (`query_type: "user"`, requête = `git config user.email`). Ne jamais coder en
  dur une identité.

## Contraintes de sortie

- **5000 caractères maximum**, tout compris. Écrire le CRA dans un fichier du
  scratchpad, mesurer avec `wc -m`, resserrer en fusionnant des puces — jamais en
  tronquant la fin.
- **Français professionnel** : invoquer le skill `french` avant de rédiger.
  Accents sur les majuscules. Anglicismes métier admis (release, backfill,
  scope), calques proscrits.
- **Texte brut** : puces avec un tiret `-`, thèmes en ligne simple suivie de `:`.
  Pas de Markdown enrichi (`**`, `##`, tableaux, liens), pas d'emoji, pas de
  numérotation dans le rendu final.
- **Undercover** : aucune mention de Claude, d'IA ou d'assistant. Les skills et
  automatisations internes se décrivent par leur effet (« vérification mensuelle
  outillée des appels de fonds »).
- **Secret des affaires** : pas de montants, de volumes d'incitations ni
  d'informations non publiques sur des entreprises tierces. Un opérateur se
  désigne par « un opérateur » quand son nom n'apporte rien.

## Procédure

### 1. Déterminer la période

- Par défaut : **le mois dernier complet** — début
  `date -I -d "$(date +%Y-%m-01) -1 month"`, fin `date -I -d "$(date +%Y-%m-01) -1 day"`.
- Mois nommé (« CRA de juillet ») : ce mois de l'année courante, sauf s'il est
  dans le futur (alors l'année précédente).
- Élargissement en cours de route (« ajoute juin ») : étendre la fenêtre et
  **renuméroter toute la liste** par date croissante, numérotation unique et
  continue sur l'ensemble de la période.
- Annoncer la fenêtre retenue (`AAAA-MM-JJ → AAAA-MM-JJ`) avant de chercher.

### 2. Collecter les tâches terminées

Critères : `État = Done`, `Date fermeture tâche` dans la fenêtre, `Personne` =
utilisateur courant.

Utiliser `notion-query-data-sources` en mode SQL — surtout pas `notion-search`,
qui ne filtre pas sur les propriétés et rate des tâches :

```sql
SELECT "userDefined:ID" AS ref,
       "date:Date fermeture tâche:start" AS fermeture,
       "Tâche" AS titre,
       url
FROM "collection://2759b461-218e-4764-ae17-0025d728193c"
WHERE "État" = 'Done'
  AND "Personne" LIKE '%<id-notion-utilisateur>%'
  AND date("date:Date fermeture tâche:start") BETWEEN '<debut>' AND '<fin>'
ORDER BY fermeture
```

`Personne` est stocké en JSON (`["user://<id>"]`), d'où le `LIKE`. Si le résultat
paraît creux (moins de 5 tâches sur un mois travaillé), relire le schéma avec
`notion-fetch` sur la data source : une propriété a pu être renommée.

### 3. Croiser avec les PR fusionnées

Toutes les PR n'ont pas de carte Notion : sans ce croisement, le CRA sous-estime
l'activité (deux mois observés : ~110 PR pour 41 cartes).

```bash
git log --since=<debut> --until=<fin+1j> --author="<prenom>" \
  --regexp-ignore-case --date=short --pretty='%cd | %s' \
  | grep -E '\(#[0-9]+\)' | sort
```

- Passer par git plutôt que `gh` : le compte GitHub n'est pas toujours
  authentifié en local.
- Rattacher chaque PR à une tâche de l'étape 2 par le sujet, et **noter le
  rattachement** : il sert à annoter les puces à l'étape 6.
- Les PR orphelines se **regroupent en chantiers** — jamais une entrée par PR.
  Dater chaque chantier à sa dernière PR et le marquer « hors Notion ».

### 4. Présenter la liste numérotée

```
01 - 2026-07-03 - Titre de la tâche
02 - 2026-07-04 - Titre de la tâche suivante
```

- Numérotation à deux chiffres, par date croissante, titres repris tels quels
  (pas de reformulation à ce stade).
- Puis demander **quelles entrées retenir**. L'utilisateur répond par numéros
  (« garde 01-05, 08 », « enlève 03 »), par volume (« les premiers pour ~12
  jours »), ou demande le complément (« les tâches restantes »). Ne rien
  présumer : attendre sa réponse.
- **Sélection par volume** : estimer une charge par entrée (0,25 / 0,5 / 1 / 2
  jours selon l'ampleur des PR), cumuler dans l'ordre chronologique, couper au
  plus près de la cible. Afficher le cumul et préciser que ce sont des
  estimations — `Nombre de jours / tâche` est presque toujours vide dans Notion.
- **Garder la trace des entrées retenues** : l'utilisateur enchaîne souvent sur
  un second CRA couvrant le reste de la période.

### 5. Grouper par thème

3 à 6 thèmes métier, dans l'ordre d'importance. Le thème nomme un chantier ou un
domaine, pas un type d'activité. Familles récurrentes du projet : plateforme de
données (datalake, dbt, Metabase), observatoire, exports et open data, espace
partenaire, campagnes et appels de fonds, conformité et sécurité, intégration
continue et outillage.

- Une entrée = une puce, sauf doublons évidents (une carte Notion et son chantier
  PR, deux cartes sur le même sujet) qu'on fusionne. Signaler les fusions.
- Un thème à une seule puce passe ; pas de « Divers » de plus de trois puces.

### 6. Rendre le CRA

```
Compte rendu d'activité - <Mois AAAA>

Thème 1 :
- puce (PR #3221)
- puce (PR #3217, #3213)

Thème 2 :
- puce
```

- **Puces nominales** : un fait par ligne, tournure au substantif
  (« Correction du décalage de date à l'export », pas « Merger la PR »). Pas de
  jargon d'outillage interne ni de nom de branche.
- **Références PR** en fin de puce, numéros seuls, jamais d'URL. Une puce sans PR
  (opération de production, action manuelle) reste sans référence : lister ces
  cas sous le bloc plutôt que d'inventer un numéro.
- Ni préambule ni conclusion dans le bloc.
- Sous le bloc : longueur (« NNNN / 5000 caractères »), nombre de puces et de
  thèmes, fusions opérées, puces sans PR.
- Corrections : l'utilisateur pointe une puce par son texte ou par le numéro de
  l'étape 4. Ne réémettre que le bloc, pas toute l'analyse.

### 7. Publier dans Notion (sur demande)

Page de publication : **CRA Malt ISN 2026**
<https://app.notion.com/p/3b3994bec93180fc97bed6459839770d>

Structure : un titre de niveau 2 par mois-année, et sous chaque titre une
sous-page par CRA, nommée `CRA <Mois AAAA> - <Prénom Nom>`.

- Créer la sous-page avec `notion-create-pages`, parent = la page ci-dessus. Le
  contenu reprend le CRA **sans la ligne de titre** (le titre de la page la
  porte) : thèmes en paragraphe suivi de `:`, puces en liste à tirets.
- Les sous-pages créées s'ajoutent en fin de page. Réordonner ensuite avec
  `notion-update-page` / `replace_content`, en plaçant chaque balise
  `<page url="...">` sous son titre de mois. Conserver toutes les balises
  existantes : en retirer une **supprime** la sous-page.
- **Mois de facturation ≠ mois travaillé** : le CRA porte le mois convenu avec
  le client, pas la fenêtre d'extraction. Demander confirmation du libellé avant
  de publier si l'utilisateur ne l'a pas donné.
- Notion est un service externe : annoncer la cible et le libellé avant d'écrire,
  puis renvoyer les URL créées.
