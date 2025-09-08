# Analyse des campagnes

Notebooks d'analyse de données des campagnes d'incitation.
Deux territoires de campagne ont été analysés :

- Pôle métropolitain du Genevois français (PMGF)
- Île-de-France Mobilités (IDFM)

## Démarrage rapide

Prérequis : Python 3.12, [uv](https://docs.astral.sh/uv/) installé.

```bash
# Depuis ce dossier
uv sync
uv run jupyter lab
```

## Sorties

- Les artefacts (graphes) et exports iront dans `./outputs/` (non versionné).

## Connexions

Si nécessaire, configurez vos variables d'environnement pour la connection à la base de données :

```bash
export DATABASE_URL="postgresql+psycopg://user:pass@host:5432/db"
```
