# Fix : le batch géo écrase `terms_violation_error` en `processed`

## Contexte

Constat équipe (ticket Notion GEN-602) : Metabase n'affiche plus de trajets
refusés pour `terms_violation_error` depuis ~mars. Hypothèse initiale : la règle
n'aurait pas été réactivée. **Cette hypothèse est fausse.**

La règle fonctionne : `verifyTermsViolation` détecte bien les violations et écrit
les labels dans `carpool_v2.terms_violation_error_labels` (labels présents chaque
mois jusqu'à aujourd'hui). Le problème est en aval : le batch de géocodage
(`processGeo`) **écrase le statut** d'acquisition.

### Cause racine

1. `registerRequest` détecte une violation -> écrit les labels ET met
   `acquisition_status = terms_violation_error`. OK.
2. Plus tard, `processGeo` traite le trajet. `CarpoolGeoRepository.findProcessable`
   sélectionne les trajets **uniquement** sur l'absence de ligne géo
   (`cg._id IS NULL`) dans une fenêtre de dates -- **sans filtrer sur
   `acquisition_status`**.
3. Après géocodage, `processGeo` appelle
   `saveAcquisitionStatus(..., Processed)` **sans condition**
   (`CarpoolAcquisitionService.ts:276-279`). `setStatus`
   (`CarpoolStatusRepository.ts`) fait un upsert sans garde -> le
   `terms_violation_error` devient `processed`.
4. La table des labels n'est jamais modifiée -> divergence labels / statut.
   Metabase lit la table `status` -> ne voit plus les violations.

Le même défaut existe sur le chemin d'erreur (`processGeo:283` met `Failed`) et
peut aussi écraser un trajet `canceled` qui n'aurait pas de ligne géo.

### Preuves (prod, 2026-06-03)

- 213 377 trajets ont un label de violation non vide mais un statut autre que
  `terms_violation_error` (213 376 `processed` + 1 `failed`), depuis 2024-10-06.
- 213 375 / 213 376 des `processed`-avec-label ont une ligne géo ; les survivants
  `terms_violation_error` n'en ont pas (géo pas encore passé).
- Délai label -> statut écrasé = backlog du batch géo (gros bloc < 48 h).
- Exemple : carpool_id 50397374 (`39a55197-be64-4573-943b-cb10b92498cf`),
  label `too_close_trips` à 08:30:34, `processed` 35 s plus tard.

Historique : présent depuis l'arrivée de la feature (PR #2637, 2024-10-07).
Ce n'est pas une régression récente. La PR #2657 a ajouté le flag
`APP_DISABLE_TERMS_VALIDATION` (non actif en prod, sinon aucun label ne serait
écrit).

## Objectif

Le géocodage doit continuer (la donnée géo est utile) mais **ne doit jamais
dégrader** un statut terminal (`terms_violation_error`, `canceled`). Seuls les
statuts géocodables (`received`, `updated`, `failed`) peuvent passer à
`processed` / `failed`.

## Implémentation (TDD)

Branche depuis `main`, PR, squash.

### 1. Test rouge d'abord

Fichier : `api/src/pdc/providers/carpool/providers/CarpoolAcquisitionService.integration.spec.ts`
(harnais existant : `makeLegacyDbBeforeAfter`, stub `GeoProvider`).

Ajouter un cas :
- enregistrer un trajet qui déclenche une violation (ex. `distance < 1000` ou
  `expired`) via `registerRequest` -> statut attendu `terms_violation_error`,
  labels écrits ;
- stubber `geoService.positionToInsee` pour renvoyer un code INSEE ;
- appeler `processGeo({ from, to, batchSize })` couvrant le trajet ;
- **assert** : `acquisition_status` reste `terms_violation_error` ET une ligne
  `carpool_v2.geo` existe pour le trajet.

Ajouter aussi un cas non-violation : trajet `received` -> après `processGeo` ->
`processed` (non-régression).

### 2. Garde au niveau du repository

Fichier : `api/src/pdc/providers/carpool/repositories/CarpoolStatusRepository.ts`

Ajouter une méthode dédiée au chemin géo qui ne touche que les statuts
géocodables (liste blanche) :

```ts
public async setGeoProcessingStatus(
  carpool_id: Id,
  status: CarpoolAcquisitionStatusEnum.Processed | CarpoolAcquisitionStatusEnum.Failed,
  client?: PoolClient,
): Promise<void> {
  const cl = client ?? this.connection.getClient();
  await cl.query(sql`
    UPDATE ${raw(this.table)}
    SET acquisition_status = ${status}
    WHERE carpool_id = ${carpool_id}
      AND acquisition_status IN ('received', 'updated', 'failed')
  `);
}
```

Note : la ligne `status` existe toujours à ce stade (créée par
`registerRequest`), donc un `UPDATE` gardé suffit (pas d'upsert). La liste
blanche `received|updated|failed` préserve `terms_violation_error` et `canceled`.

### 3. Utiliser la garde dans `processGeo`

Fichier : `api/src/pdc/providers/carpool/providers/CarpoolAcquisitionService.ts`
(lignes 276-279 et 283-286)

Remplacer les deux `saveAcquisitionStatus({ ..., Processed/Failed })` du bloc
`processGeo` par `setGeoProcessingStatus(item.carpool_id, ...Processed/Failed)`.
Ne pas toucher `geoRepository.upsert` (le géocodage doit rester inconditionnel).

Laisser inchangés les `saveAcquisitionStatus` de `registerRequest`,
`patchCarpool`, `cancelRequest`.

### 4. Vérification

```sh
# tests ciblés (depuis api/)
just test api/src/pdc/providers/carpool/providers/CarpoolAcquisitionService.integration.spec.ts
# ou la commande deno test du repo
```

Vérif manuelle post-déploiement (lecture prod) : un nouveau trajet avec violation
doit garder `acquisition_status = terms_violation_error` après passage du batch
géo, et posséder une ligne `carpool_v2.geo`.

## Données historiques

Les 213 377 trajets déjà écrasés ne sont pas corrigés par ce changement de code.
Voir `docs/fixes/backfill-terms-violation-status.sql` (à exécuter
délibérément, **après** décision équipe sur les effets aval -- incitations déjà
versées, etc.).

## Référence

- Ticket : Notion GEN-602 « Vérification bon fonctionnement
  aquisition_terms_violation_error »
- Metabase : Q716 (trajets term violation flaggés processed), Q444 (refus/mois)
- PR historiques : #2637 (ajout feature), #2657 (flag de désactivation)
