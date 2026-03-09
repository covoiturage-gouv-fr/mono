![Registre de preuve de covoiturage](https://rpc.s3.fr-par.scw.cloud/rpc-large.png)

Le Registre de preuve de covoiturage est un produit [beta.gouv.fr](https://beta.gouv.fr) qui a pour but de certifier qu'un covoiturage a bien eu lieu. L'objectif de l'outil est d'agir en tant que tiers de confiance entre les différents acteurs du covoiturage (opérateurs, autorités organisatrices de mobilité, entreprises, régions, etc.) pour simplifier la mise en place d'incitations à destination des covoitureuses et covoitureurs. Cette Startup d'État a pour objectif d'aider à réduire l'auto-solisme et l'emprunte écologique des déplacements courts.

- Plus d'informations sur le [Registre de preuve de covoiturage](https://covoiturage.beta.gouv.fr/)
- [Espace partenaire (territoires/opérateurs)](https://partenaire.covoiturage.beta.gouv.fr)
- [Documentation générale](https://doc.covoiturage.beta.gouv.fr)
- [Documentation technique](https://tech.covoiturage.beta.gouv.fr)
- [Statistiques publiques](https://covoiturage.beta.gouv.fr/statistiques)
- [Metabase (interne)](https://stats.covoiturage.beta.gouv.fr)
- [Matomo (interne)](https://matomo.covoiturage.beta.gouv.fr)
- [Page de statut des services](https://status.covoiturage.beta.gouv.fr)
- [Dépôt de l'application](https://github.com/covoiturage-gouv-fr/mono)
- [Dépôt de l'infrastructure](https://github.com/covoiturage-gouv-fr/infra)

### Sécurité

Politique de sécurité et contact : [SECURITY.md](SECURITY.md)

### Stack technique

1. Cloner le repo et `cd` dedans
2. `cp api/.env.example api/.env`
3. Modifier `api/.env`
4. `just docker build`

> **Note**
> Sur NixOS, il faut ajouter `DOCKER_SOCK=/run/user/1000/docker.sock` dans `api/.env`
> pour faire fonctionner Traefik correctement

#### Migrations

```shell
# Préfixer avec `just dc run api` pour exécuter dans le container de l'API
just migrate    # Migrer les schemas de données
just source     # Migrer les données géographiques
just seed       # Remplir la base de données avec des données de test
just seed-local-users # Ajouter des utilisateurs pour les tests (NODE_ENV=local)
```

#### Contrôle des services

Les différents fichiers `docker-compose.*.yml` sont utilisés en _overlay_ pour créer la stack de dev.

- `base`: définition des services sur localhost. Pas de ports exposés
- `proxy`: ajout d'un proxy Traefik pour exposer les services sur `*.covoiturage.test`
- `e2e`: overlay utilisé pour les tests de bout en bout
- `dev`: ouvre les ports pour le développement local

#### Examples de commandes

- Lister les commandes disponibles: `just`
- Lancer la stack de dev en localhost (sans l'API): `just dev | just start | just up`
- Démarrer l'API en local: `just serve`
- Lancer la stack sur les URL `*.covoiturage.test` (avec l'API): `just proxy`
- Utiliser Docker dans l'environnement: `just docker <docker_command>`
- Afficher les logs des services: `just logs [<service>] [-f]`
- Stopper les services: `just stop | just down`

#### Misc

- Se connecter à la DB avec pgcli: `just db`
- Lancer un REPL avec le kernel de l'API: `just debug`
- Ajouter la résolution des domaines `*.covoiturage.test` dans `/etc/hosts`: `just add-hosts`
- Ajouter des utilisateurs pour les tests: `just seed-local-users`
- Lister les variables d'environnement de l'application: `just env`
- Lancer les tests de l'API en local: `just test`, `just test-integration`, `just test-unit`
- Lancer la stack de tests d'intégration: `just ci_test_integration`
- Lancer la stack de tests bout-en-bout: `just ci_test_e2e`

> **Note**
> Les stacks de tests CI sont lancées sur des volumes Docker qui sont détruits à la fin des runs.

### Versions

Le code suit les spécifications [semver](https://semver.org/).

# License

DINUM / DGITM / ADEME, 2017-2026

The source code is published under [Apache license 2.0](./LICENSE).
