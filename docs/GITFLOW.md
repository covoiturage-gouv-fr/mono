# Workflow git et releases

## Branches

| Branche | Rôle | Release produite | Déploiement |
| ------- | ---- | ---------------- | ----------- |
| `main` | code en production | stable `vX.Y.Z` | demo + production |
| `next` | évolutions à valider en demo | prérelease `vX.Y.Z-rc.N` | demo seulement (API, espace partenaires, observatoire) |
| `feature` | travail en cours (worktree) | aucune | aucun |

```text
feature ──squash──▶ next ──merge commit──▶ main
                     ▲                      │
                     └────merge commit──────┘
```

## Méthode de fusion

| PR | Méthode |
| -- | ------- |
| `feature` → `main` | squash |
| `feature` → `next` | squash |
| `next` → `main` | **merge commit** |
| `main` → `next` | **merge commit** |

Le ruleset « Protect base branches » autorise merge commit et squash sur `main` et `next`. Il ne sait pas filtrer sur la branche source : **le choix de la méthode est manuel**, et GitHub présélectionne la dernière utilisée. Vérifier le bouton avant de fusionner.

Pourquoi un merge commit entre `main` et `next` :

- `next` → `main` : semantic-release lit les commits. Un squash ne garde que le titre de la PR et perd les feat/fix des rc.
- `main` → `next` : le tag stable de `main` doit être atteignable depuis `next`, sinon semantic-release refuse de publier (`EINVALIDNEXTVERSION`).

Jamais de rebase de `next` sur `main` : le force-push est bloqué, et les tags `-rc.N` déjà publiés deviennent inatteignables (semantic-release recalcule un `rc.1` qui existe déjà).

## Ce qui déclenche une release

Deux conditions, toutes les deux nécessaires (squash : le titre de la PR devient le message du commit) :

1. **Fichiers** : le diff touche `api/`, `app-partners/`, `app-observatory/`, `shared/` ou `docker/api/` (job `changes` de `quality.yml`).
2. **Type de commit** : `feat` (mineure), `fix` / `perf` / `revert` (corrective), `!` ou `BREAKING CHANGE` (majeure). Les scopes `dbt`, `datalake` et `cms` ne publient jamais.

La CI doit être verte : un job en échec bloque le job `release`.

## Procédures

### Livrer directement

PR `feature` → `main` en squash. La stable part en demo et en production.

### Faire valider en demo

1. PR `feature` → `next` en squash : `vX.Y.Z-rc.N` part en demo (environ 10 min, délai de scan Flux).
2. Une fois validé, PR `next` → `main` en merge commit : la stable part en demo et en production.
3. Rattrapage : PR `main` → `next` en merge commit.

### Rattrapage de `next`

Après **chaque** release stable publiée depuis `main`, y compris celles venues d'une PR `feature` → `main` : PR `main` → `next` en merge commit, avant tout autre merge dans `next`.

## Remédiations

Principe commun : **refaire la même PR et la fusionner en merge commit**. Après un squash, les commits source ne sont pas des ancêtres de la cible : GitHub accepte la nouvelle PR, le contenu est déjà identique et la fusion ne sert qu'à raccrocher les historiques.

### `main` → `next` fusionnée en squash

- **Sans nouveau tag sur `main` depuis le dernier rattrapage** : sans conséquence.
- **Avec un tag stable sur `main`** : il n'est pas atteignable depuis `next`, le prochain rc échoue ou prend un mauvais numéro. Nouvelle PR `main` → `next` en merge commit, avant tout autre merge dans `next`.

### `next` → `main` fusionnée en squash, sans release

Titre non publiant (`chore`, `ci`…) : les feat/fix des rc ne sont pas comptés. Nouvelle PR `next` → `main` en merge commit : les commits de `next` entrent dans l'historique de `main` et la stable est publiée.

### `next` → `main` fusionnée en squash, avec release

La production est correcte, mais les commits d'origine de `next` n'ont pas été comptés par cette release. Au prochain merge `next` → `main`, ils le seront une seconde fois (version en trop, changelog en double). Au choix :

- accepter : sans gravité ;
- réaligner `next` sur `main`, quand `next` n'a rien en attente (administrateur seulement, équipe prévenue) :
  1. dans « Protect base branches », ajouter le rôle Repository admin en bypass (ne pas désactiver le ruleset : `main` resterait sans protection) ;
  2. `git push --force-with-lease=next:<sha actuel de next> origin main:next` ;
  3. retirer le bypass et vérifier le ruleset (Settings → Rules).

  Les rc de la version publiée sont de toute façon dépassés.

### Prérelease abandonnée

La demo suit la plus haute version : pas de retour arrière automatique. Publier une version supérieure, ou suspendre l'automatisation Flux côté infra. Le schéma de la base demo reste migré jusqu'au prochain reset.

## Points d'attention

- Jamais de tag posé à la main : cela fausse le calcul de la version suivante.
- Une prérelease migre la base demo : ses migrations doivent rester compatibles avec la stable suivante.
- Les fronts demo suivent la dernière publication : une stable publiée pendant qu'un rc est en demo les remplace, alors que l'API demo reste sur le rc.
