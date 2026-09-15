# Territoires custom — déclarations

Un fichier `*.yml` par territoire *composite* (somme d'AOM / EPCI / départements /
régions / communes). Ces fichiers sont la **source humaine** ; ils ne sont jamais lus
par dbt directement.

## Cycle de vie

1. Ajouter / modifier un `<slug>.yml` ici.
2. `just custom-territories-compile` — résout les membres contre `perimeters` au dernier
   millésime et réécrit les seeds `seeds/trusted/custom_territories.csv` et
   `custom_territories_meta.csv`.
3. Commiter **le YAML et le diff des seeds** dans la même PR (l'intention + l'effet).
4. Déployer : `just pipeline-trusted-geo` (re-seed + rebuild `perimeters_agg`). Le calcul
   agrégé complet d'un nouveau composite nécessite un backfill dédié (phases suivantes).

`just custom-territories-compile check` recompile en mémoire et échoue si les seeds
commités divergent (garde-fou).

## Format

```yaml
id: pole-metropolitain        # slug ^[a-z0-9-]{3,32}$, jamais purement numérique
libelle: Pôle métropolitain XYZ
active: true                  # false = présent dans les seeds mais absent de perimeters_agg
members:                      # au moins un membre non vide
  arr:  ["38240"]             # codes commune/arrondissement — À QUOTER (zéros, "2A"/"2B")
  epci: [225236547]           # SIREN EPCI
  aom:  [200053767]           # code AOM
  dep:  []
  reg:  []
```

- `arr` : rattaché tel quel (existence vérifiée au millésime courant).
- `com` / `epci` / `aom` / `dep` / `reg` : résolus vers toutes leurs communes (`arr`).
- Recouvrement de membres : chaque commune n'est comptée **qu'une fois** par composite.
- `earliest_safe_start` (dans le seed `_meta`) : borne de backfill sûre, calculée depuis
  `com_evolution` (fusions de communes) ; plancher `2019-01-01`.

L'association d'un SIRET (agence/entreprise) à un territoire pour l'autorisation ProConnect
n'est **pas** portée ici : ce n'est pas un besoin de composition géographique. Ne créez pas
de territoire custom pour un périmètre qui existe déjà (ex. un département seul) juste pour
lui attacher un SIRET.
