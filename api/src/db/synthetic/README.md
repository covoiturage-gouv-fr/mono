# Jeu de données synthétique

`just seed-synthetic <profil> [YYYY-MM-DD]` remplit une base avec un jeu de données fictif, décrit par un profil de
`profiles/`. Rejouable (aucun doublon) et reproductible (graine fixe du profil, aucun tirage ni horloge hors du profil
et de la date de référence).

À ne pas confondre avec `just seed`, le petit jeu des tests d'intégration et e2e.

## Prérequis

1. `just migrate` : schéma à jour.
2. `geo.perimeters` chargée par `geo-import` (`../geo/RUNBOOK.md`). Le générateur ne charge aucune géographie et
   s'arrête si les périmètres du profil sont absents.
3. `APP_ENV` dans la liste `allowedEnvs` du profil. `production` est toujours refusé.

## Ce qui est créé

| Objet        | Tables                                                                                                        | Clé de rejeu                                |
| ------------ | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| Entreprises  | `company.companies`                                                                                           | SIRET                                       |
| Opérateurs   | `operator.operators`                                                                                          | SIRET                                       |
| Territoires  | `territory.territory_group`, `territory.territory_perimeters` (même logique que `territory:perimeter create`) | nom                                         |
| Utilisateurs | `auth.users`, `auth.user_scopes`                                                                              | e-mail                                      |
| Campagnes    | `policy.policies`                                                                                             | territoire + handler                        |
| Trajets      | `carpool_v2.carpools`, `status`, `geo`, `requests`                                                            | opérateur + `operator_journey_id` (`syn-…`) |

Les trajets couvrent les `months` mois civils qui précèdent la date de référence. Origines et destinations sont tirées
autour du centroïde des communes du périmètre (dernier millésime).

Statuts générés : `ok` (processed / passed / passed), `fraud_error` (fraude `failed`), `anomaly_error` (anomalie
`failed`), `expired`, `canceled`. Une part `anomalyRate` des trajets est volontairement incohérente : vitesse
irréaliste, conducteur et passager identiques, doublon du trajet précédent. Aucune ligne n'est écrite dans
`fraudcheck.labels` ni `anomaly.labels`.

Non créé : les clés applicatives des opérateurs (un administrateur les crée depuis le dashboard), les incitations, les
exports et les appels de fonds (commandes ci-dessous).

## Après le seeding

La commande affiche les identifiants créés (`operators`, `territories`, `policies`). Avec le profil `pentest` et sa date
de référence (trajets de 2025-11 à 2026-10) :

```shell
# incitations : calcul puis finalisation sur la période des trajets
just api campaign:apply -c <policy_id>,<policy_id> -f 2025-11-01 -t 2026-11-01
just api campaign:finalize -f 2025-11-01 -t 2026-11-01

# appel de fonds d'un mois clos
just api apdf:export -c <policy_id> -s 2026-10-01 -e 2026-11-01

# demande d'export, par territoire puis par opérateur (le fichier est produit par le worker d'export)
just api export:create -c <user_id> -g aom:287500078 -s 2026-10-01 -e 2026-11-01
just api export:create -c <user_id> --target operator -o <operator_id> -s 2026-10-01 -e 2026-11-01
```

Les campagnes s'appuient sur les handlers existants de `services/policy/engine/policies` : un trajet n'est incité que
s'il respecte leurs règles (opérateur, classe, distance, géographie). Les handlers reconnaissent les opérateurs à leur
UUID, d'où les UUID d'`OperatorsEnum` dans le profil. `campaign:apply` passe en `finished` les campagnes actives dont la
fin est dépassée à l'horloge réelle.

## Ajouter un profil

Créer `profiles/<nom>.ts` (type `SyntheticProfile`) et l'enregistrer dans `profiles/index.ts`. Les tests unitaires
vérifient chaque profil : SIRET valides, handlers connus, `allowedEnvs` limité à `local` et au nom du profil.

`email` et `login_siren` des utilisateurs sont de simples données du profil : à la connexion ProConnect, le compte est
cherché par e-mail puis le SIREN renvoyé est comparé à `login_siren` (sauf `registry.admin`). Les aligner sur les
identités du fournisseur d'identité utilisé.
