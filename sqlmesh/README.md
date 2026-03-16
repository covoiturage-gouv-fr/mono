# RPC Data Warehouse

Le Data Warehouse de Preuve de Covoiturage (RPC) est une solution de gestion et d'analyse des données conçue pour stocker, organiser et interroger efficacement les données liées aux activités de covoiturage. Il utilise SQLMesh, un framework de gestion de modèles SQL, pour structurer et orchestrer les transformations de données.

- [Documentation de SQLMesh](https://sqlmesh.readthedocs.io/en/stable/)

## Installation

SQLMesh est une librairie Python. Elle nécessite un environnement virtuel (venv) et l'installation de quelques dépendances.

Le shell Nix à la racine du monorepo configure et active l'environnement virtuel automatiquement. Les dépendances sont installées automatiquement avec `uv`.

```shell
nix develop
(nix-shell) cd ./sqlmesh
```

**Installation manuelle**

Prérequis : installer [Python](https://www.python.org/downloads/) et [uv](https://docs.astral.sh/uv/).

```shell
cd ./sqlmesh
uv venv
uv sync
```

## Configuration

Copier `.env.example` et modifier le fichier `.env`.

```shell
cp .env.example .env
```

## Utilisation de SQLMesh

Les données sont organisées en 3 zones :

- **archive** : Consolidation des données du RPC par années + backup au format `.parquet` sur S3.
- **raw** : Données brutes importées depuis les sources externes + archives RPC.
- **trusted** : Données transformées et nettoyées, prêtes pour l'analyse.
- **refined** : Données finales pour les utilisateurs.

Les modèles SQL sont définis dans le dossier `models/`. Chaque zone a son propre sous-dossier. L'organisation des dossiers n'influe pas sur les dépendances entre modèles qui est définie dans chacun d'eux.

### Commandes utiles

Les exemples suivants utilisent l'environnement de développement `dev`.

```shell
# Analyser les modifications apportées aux modèles SQL
$ sqlmesh plan dev
```

```shell
# Appliquer les modifications planifiées automatiquement
$ sqlmesh plan dev --auto-apply
```

```shell
# Executer une requête sur les données d'un modèle spécifique
$ sqlmesh fetchdf "SELECT * <zone>[__<env>].<table>"
```

> On suffixe `<zone>` par `__` + l'environnement s'il est différent de `prod`.

Exemples :

```shell
sqlmesh fetchdf "SELECT * FROM raw_zone__dev.aires_covoiturage"
sqlmesh fetchdf "SELECT * FROM refined_zone.part_campaigns_by_month"
```
