# GEN-643 — APDF : intégration du delta déclaré vs calculé

> Plan & audit d'impact / complexité. Branche : `worktree-GEN-643-apdf-delta`.
> Ticket Notion : [APDF - Intégration delta déclaré vs calculé](https://app.notion.com/p/37c994bec93180559a8ed559d18d598d) (Priorité 🔥 High).

## 1. Objectif (rappel du besoin)

Les collectivités ne peuvent pas, avec le seul fichier APDF actuel (données **calculées** par covoit.beta), repérer les décalages avec les factures / chiffres **déclarés** par les opérateurs.

Besoin : faire apparaître dans le fichier APDF, sur l'onglet de synthèse, la valorisation du **delta entre données calculées (RPC) et déclarées (opérateur)**.

Draft de spec (ticket) :
- ajouter dans le tableau des tranches des colonnes avec les données **déclarées** opérateur utiles aux collectivités : **montant incitation, volume de trajets incités, contribution passager** ;
- **distinguer visuellement** calculé vs déclaré (code couleur) ;
- ajouter une **synthèse du différentiel** (montant incité + volume trajets incités) entre covoit.beta et opérateur.

> Le draft détaillé est fourni (`~/apdf.xlsx`, onglet **« Proposition évolution »**) : c'est la sortie APDF de production avec un onglet maquette inséré. Maquette analysée ci-dessous (§2.6). Reste l'échange Vic/Jo/Éric/Thomas pour valider.

### Maquette cible (onglet « Proposition évolution ») — voir §2.6 pour le détail
Onglet Synthèse passe de **5 colonnes (A-E)** à **7 colonnes (A-G) groupées en 2 bandeaux**, + un **bloc Delta**.

## 2. État des lieux technique (audit du code & des données)

### 2.1 Chaîne d'export APDF
- Commande CLI `apdf:export` → `commands/ExportCommand.ts:20`
- → `actions/ExportAction.ts:33` (boucle campagne × opérateur, upload S3)
- → `providers/excel/BuildExcel.ts:35` construit le classeur (2 onglets) :
  - **« Synthèse par tranche »** → `providers/excel/SlicesWorksheetWriter.ts`
  - **« Trajets »** → `providers/excel/TripsWorksheetWriter.ts`
- Données : `providers/DataRepositoryProvider.ts`
  - `getPolicyStats()` → bornes/labels des tranches (lignes du tableau de synthèse)
  - `getPolicyCursor()` → lignes détaillées de l'onglet Trajets

### 2.2 Mécanique clé de l'onglet Synthèse (déterminante pour le plan)
**L'onglet « Synthèse par tranche » est piloté à 100 % par des formules Excel** (`SUMIFS`/`COUNTIFS`) qui pointent vers les colonnes de l'onglet **Trajets** — voir `SlicesWorksheetWriter.ts:269-322`. Références par lettre de colonne :
- `M` = distance · `R` = rpc_incentive (calculé) · `S` = incentive_type · `T` = passenger_contribution

`getPolicyStats()` ne fournit que la liste des tranches (start/end + label) ; **les valeurs affichées sont calculées vivantes par Excel** sur l'onglet Trajets.

→ **Conséquence : pour ajouter une donnée à la synthèse, on ajoute d'abord une colonne à l'onglet Trajets, puis on l'agrège par `SUMIFS`/`COUNTIFS`.** C'est exactement le schéma de la PR #3206 (ajout de `passenger_contribution`, colonne T).

### 2.3 Précédent direct : PR #3206 (`passenger_contribution`)
Patron de référence complet (data pass-through SQL → normalize → cellule Excel) :
- interface `APDFTripInterface` (+1 champ)
- SQL `getPolicyCursor()` (+1 colonne)
- `normalizeAPDFData.helper.ts` (conversion centimes→euros + null-safe)
- `TripsWorksheetWriter.ts` (entête + largeur colonne)
- `SlicesWorksheetWriter.ts` (entête + format € + `SUMIFS` + total + définition de champ)
- test unitaire de normalisation

### 2.4 ✅ La donnée « déclarée » EXISTE déjà (pas d'import à construire)
C'est le point le plus important de l'audit. Le besoin laissait craindre une source de données inexistante. **Faux** : les incitations déclarées par les opérateurs sont stockées au niveau trajet dans :

**`carpool_v2.operator_incentives`** — colonnes : `carpool_id`, `idx`, `siret`, `amount` (centimes).
- Jointure : `operator_incentives.carpool_id = carpool_v2.carpools._id`
- « contribution passager déclarée » = `carpool_v2.carpools.passenger_contribution` (déjà exporté).

Donc le « déclaré » et le « calculé » sont déjà tous deux en base. **Aucun mécanisme d'import / formulaire / API à créer.** C'est ce qui rend le chantier raisonnable.

### 2.5 Quelles incitations déclarées comparer ? → **résolu : appariement par SIREN du territoire**
Volumétrie : la table `operator_incentives` est volumineuse (plusieurs dizaines de millions de lignes) → **un trajet porte souvent plusieurs incitations déclarées** de **financeurs différents** (région + département + AOM…). Le calculé RPC d'une campagne ne concerne qu'**un** financeur : le territoire qui porte la campagne. Sommer toutes les incitations sur-compterait.

**Décision (consigne métier confirmée)** : le financeur = **le territoire qui définit la campagne**. Chaîne de résolution validée en base :

```
policy.policies.territory_id
  → territory.territory_group.company_id
    → company.companies.siren     (et .siret)
```

**Niveau d'appariement = SIREN (9 chiffres), pas le SIRET complet.** Vérifié sur données réelles :
- Le SIRET « siège » du territoire **ne matche pas** : les opérateurs déclarent sous d'**autres établissements du même SIREN** (NIC différent). D'où l'appariement sur les 9 premiers chiffres.
- Filtre SIRET complet → **0 match**. Filtre SIREN → match correct. Filtre « tous sirets » → sur-compte.

Comparaison réalisée en base sur des campagnes témoins (montants non reproduits — **dépôt public**) : le filtre **SIRET complet → 0 correspondance**, le filtre **SIREN → delta cohérent et exploitable**, le filtre **« tous sirets » → sur-comptage** (capte d'autres financeurs).

→ Le delta SIREN est plausible et exploitable ; c'est le bon périmètre. (Cohérent avec la logique `siret_is_aom` de la couche analytics existante.)

**Cas limites à gérer/documenter** : territoire sans `company_id` (→ déclaré nul, documenté) ; délégation à un syndicat de SIREN différent (non apparié — à documenter) ; un même SIREN finançant plusieurs campagnes.

### 2.6 Maquette cible détaillée (onglet « Proposition évolution »)

**Onglet « Synthèse par tranche » — nouvelle structure.** Une ligne de bandeaux est insérée au-dessus des entêtes :

| | Bandeau « **Données calculées par covoiturage.beta** » | | | Bandeau « **Données déclarées par les opérateurs** » | | |
|---|---|---|---|---|---|---|
| **A** Tranche | **B** Montant d'incitation | **C** Tous les trajets | **D** Trajets incités | **E** Montant d'incitation | **F** Trajets incités | **G** Contribution passagers |

- B/C/D = calculé RPC (existant) : `SUMIFS(Trajets!R:R,…)`, `COUNTIFS(…)`, `COUNTIFS(Trajets!R:R,">0",…)`.
- **E** = montant déclaré : `SUMIFS(Trajets!U:U,…)` (nouvelle colonne U, cf. §4).
- **F** = trajets incités déclarés : `COUNTIFS(Trajets!U:U,">0",…)`.
- **G** = contribution passagers (déplacée de E→G) : `SUMIFS(Trajets!T:T,…)`.
- Deux tableaux : « période normale » puis « période booster », chacun avec ligne de total.

**Bloc Delta (sous les tableaux) :**
- Libellé « **Delta données covoiturage.beta / opérateurs** » sur 2 mesures : **Montant d'incitation** et **Trajets incités** (calculé − déclaré).
- Note obligatoire : *« un écart de données peut exister du fait d'application de règles métier différentes entre l'opérateur et covoiturage.beta.gouv »*.
- (Dans la maquette les cellules delta sont en `#NAME?` — placeholder ; à implémenter en formules sur les totaux B/E et D/F.)

**Distinction calculé/déclaré (« code couleur » du ticket)** = les deux bandeaux d'entête fusionnés + remplissage de couleur distinct par bandeau (fills ExcelJS).

> ⚠️ Dans la maquette, le tableau **booster** n'a PAS les colonnes déclarées E/F (seulement B/C/D/G). À confirmer : appliquer la même structure au booster (cohérence) ou la limiter au normal comme dessiné.

## 3. Règle de calcul retenue

- **Incitation déclarée (territoire)** = Σ `carpool_v2.operator_incentives.amount` du trajet **où `left(siret,9) = SIREN du territoire de la campagne`**.
- **Incitation calculée (RPC)** = `policy.incentives.amount` (existant, colonne `R` de l'onglet Trajets).
- **Trajets incités déclarés** = nb de trajets dont l'incitation déclarée (SIREN) > 0.
- **Delta** = calculé RPC − déclaré territoire (montant et volume), par tranche + total.
- **Contribution passager** : déjà exportée (déclarée par construction).

## 4. Plan d'implémentation (sous réserve décision §3)

Hypothèse de travail : on suit le patron PR #3206. Tout est dans `api/src/pdc/services/apdf/`.

1. **SQL — résolution du SIREN territoire + montant déclaré**
   - Résoudre une fois le SIREN du territoire de la campagne : `policy.policies.territory_id → territory.territory_group.company_id → company.companies.siren`. Le faire dans `ExportAction` (passé en param) ou en CTE de la requête.
   - `DataRepositoryProvider.getPolicyCursor()` (`providers/DataRepositoryProvider.ts:162`) : ajouter
     `LEFT JOIN LATERAL (SELECT sum(oi.amount) FROM carpool_v2.operator_incentives oi WHERE oi.carpool_id = cc._id AND left(oi.siret,9) = :territory_siren) decl(amount)` → `operator_declared_incentive`.
   - Idem dans `getPolicyStats()` si on veut les valeurs côté serveur (sinon laisser les formules Excel agréger la nouvelle colonne Trajets).
   - Vérifier l'index sur `carpool_v2.operator_incentives(carpool_id)` (jointure sur une table volumineuse).
2. **Interface** — `interfaces/APDFTripInterface.ts` : `+ operator_declared_incentive: number | null`.
3. **Normalisation** — `helpers/normalizeAPDFData.helper.ts` : conversion centimes→euros, null-safe (+ test unitaire dédié, cf. `normalizeAPDFData.unit.spec.ts`).
4. **Onglet Trajets** — `providers/excel/TripsWorksheetWriter.ts` : nouvelle colonne **`U` = `operator_declared_incentive`** (montant déclaré SIREN, en euros), entête + largeur (patron col. T). C'est la source des `SUMIFS`/`COUNTIFS` du déclaré.
5. **Onglet Synthèse** — `providers/excel/SlicesWorksheetWriter.ts`, refonte de `drawSliceTable` selon §2.6 :
   - insérer la **ligne de bandeaux** fusionnés (`B:D` = « Données calculées… », `E:G` = « Données déclarées… ») avec **fills de couleur distincts** (le « code couleur ») ;
   - colonnes : **B/C/D** calculé (existant), **E** = `SUMIFS(Trajets!U:U,…)`, **F** = `COUNTIFS(Trajets!U:U,">0",…)`, **G** = `SUMIFS(Trajets!T:T,…)` (contribution, déplacée de E→G) ;
   - mettre à jour `numFmt €` (B & E & G), largeurs, et la **ligne de total** (somme B,E ; comptes C,D,F) ;
   - **bloc Delta** : libellé + 2 formules (`total B − total E`, `total D − total F`) + la **note** sur les écarts de règles métier ;
   - **définitions de champs** : ajouter l'entrée du montant déclaré (et préciser le périmètre SIREN territoire) ;
   - reproduire pour les 2 tableaux (normale + booster, cf. question booster §2.6).
   - ⚠️ l'insertion du bandeau décale toutes les lignes de +1 → recaler les références de la doc (`A20`, `G1`…) et les plages de total.
6. **Tests** :
   - unit normalize (nouveau champ) ;
   - integration `DataRepositoryProvider.integration.spec.ts` (jointure operator_incentives) ;
   - unit `SlicesWorksheetWriter` / `BuildExcel` (présence colonnes/formules).
7. **Docs** :
   - `api/specs` (publication bump.sh) + documentation publique des champs APDF ;
   - `api/src/pdc/services/apdf/README.md` si présent.

## 5. Audit d'impact

| Zone | Impact |
|---|---|
| Schéma BDD | **Aucun** (lecture seule de tables existantes) |
| SQL `getPolicyCursor` | +1 jointure latérale → coût requête à surveiller (table volumineuse ; index sur `operator_incentives.carpool_id` à vérifier) |
| Format fichier APDF | **Changement visible client** : nouvelles colonnes + delta. Impacte les collectivités qui parsent le fichier → communication / changelog |
| Onglet Synthèse | Réagencement colonnes (risque de régression sur formules/totaux/fusions) |
| Autres exports (IDFM custom #3206) | Vérifier qu'un export spécifique ne casse pas avec le nouveau schéma de colonnes |
| Perf export | Marginal par trajet ; valider sur grosse campagne |
| Rétrocompat fichiers passés | Les anciens fichiers restent inchangés ; nouveaux mois seulement |

## 6. Audit de complexité

**Complexité technique : Moyenne (revue à la baisse).** Patron PR #3206 réutilisable, pas de nouvelle infra, données présentes, **mapping campagne→SIREN résolu et validé en base**.

Postes de complexité, par ordre :
1. **Réagencement de l'onglet Synthèse** — formules Excel par lettre de colonne + fusions/totaux codés en dur : sensible aux régressions, à tester sur fichier réel ouvert dans Excel/LibreOffice. C'est désormais le poste le plus délicat.
2. **Appariement SIREN — cas limites** — territoire sans `company_id`, délégation à un SIREN tiers, SIREN finançant plusieurs campagnes : à documenter clairement dans l'onglet définitions pour éviter une mauvaise lecture du delta.
3. **Performance SQL** — jointure latérale sur une table volumineuse ; vérifier l'index `operator_incentives(carpool_id)` et le coût du `left(siret,9)`.
4. **Code couleur** — simple en soi (fills ExcelJS), mais multiplie les cellules à styler.

**Effort estimé** : ~1–2 j de dev + tests, une fois le draft de spec détaillé (onglet 2) intégré. Plus de chemin critique « décision data » : le périmètre est tranché.

## 7. Hors-scope / questions ouvertes
- Indicateurs + alertes Metabase (évoqués dans le ticket parent GEN-441) → **hors scope** de ce fichier APDF.
- Lecture du draft de spec détaillé (onglet 2 du Google Sheet) — à intégrer avant dev.
- Validation du périmètre SIREN avec Vic/Jo/Éric/Thomas (confirmer que l'appariement par SIREN du territoire répond bien au besoin des collectivités).
- Confort de lecture : faut-il aussi exposer le déclaré **ligne à ligne** dans l'onglet Trajets, ou seulement agrégé en synthèse ?
- Détail par financeur des incitations `idx` multiples (somme SIREN retenue ; faut-il aussi montrer le « tous financeurs » à titre indicatif ?).
