# Le Frigo — refroidissement RGPD des données du Registre

> Statut : **ossature validée** (2026-07-13). D = 15 mois confirmé. Restent 2 décisions externes (§8) à porter côté DPO / AOM ; non bloquantes pour le plan d'implémentation.

## 1. Problème

Le Registre (RPC) ingère des trajets contenant de la donnée personnelle brute (téléphone,
nom travelpass, plaque, position GPS exacte, payload opérateur original). Le RGPD impose de
supprimer cette donnée le plus rapidement possible **sans perdre** la capacité de :

- recalculer les incitations de campagnes (trajets envoyés en retard, campagne modifiée
  rétroactivement, nouvelle campagne limitrophe ajoutée après coup) ;
- produire les agrégats non identifiants (datalake, opendata) ;
- honorer les appels de fonds (APDF) — résultats d'incitation.

## 2. Principe directeur

Supprimer la ligne brute identifiante de la base opérationnelle à un seuil **D** (voir §4),
sans jamais perdre les capacités ci-dessus.

**Décision structurante : pas de « frigo » offline du raw.** Analyse du code : aucun
consommateur n'a besoin du payload brut ou des identifiants directs au-delà de la fenêtre
chaude (voir §7). Archiver le raw « au cas où » serait de la sur-conservation RGPD sans
finalité déterminée. On **supprime**, on n'archive pas. (Le seul cas qui justifierait une
rétention ciblée est l'export partenaires > D — décision ouverte §8.)

## 3. Architecture — 3 paliers

| Palier | Contenu | Rétention | Nature RGPD |
|---|---|---|---|
| 🔥 **CHAUD** — `carpool_v2.*` + `requests.payload` | tout, PII brute incluse | **D = 15 mois** glissants (§4) | identifiant direct |
| 🌤️ **DATALAKE** — `zone_trusted` + `aggregated/*` + `exposed/*` | hash identité + H3 + INSEE + datetime + distance + montants | longue, **matérialisée** | pseudonyme → anonyme |
| 🌍 **PUBLIC** — opendata data.gouv.fr | k-anonymisé, coords arrondies | illimité | anonyme (hors RGPD) |

Flux : `carpool_v2` (chaud) → FDW → `zone_trusted` (pseudonymisé, matérialisé) → étages
supérieurs → exports. La suppression du chaud à D ne fait rien perdre car le datalake a déjà
matérialisé ce qui doit survivre.

## 4. Fenêtres temporelles

Deux fenêtres à **découpler** (`D > R + marge`) :

- **R = fenêtre de recalcul / rétroactivité = 12 mois.** Une campagne ne peut jamais toucher
  un trajet plus vieux que 12 mois. Le moteur d'incitation relit `carpool_v2` (cf. §7) : tout
  trajet recalculable doit donc être présent dans le chaud jusqu'à 12 mois.
- **D = seuil de suppression du raw = 15 mois** (R + 3 mois de marge opérationnelle : batchs
  en retard, finalize tardif, campagne créée à la dernière minute). *Confirmé.*

Un `D = R` (12 = 12) laisserait un trajet recalculable au bord exact de la suppression : à
proscrire.

### Cap dur de rétroactivité — clé de voûte

`R = 12 mois` n'a de sens que s'il est **imposé dans le moteur** : refuser toute campagne (ou
modification de campagne) dont la portée vise des trajets > 12 mois. Sans ce cap, un jour
quelqu'un crée une campagne rétroactive à 18 mois et toute la logique du frigo s'écroule
(données déjà supprimées). Le cap doit être une **règle métier explicite et testée**, pas une
convention implicite.

## 5. Le job de refroidissement (à D = 15 mois)

Suppression **de la ligne entière** du trajet (pas un simple `NULL` de colonnes) :
`carpool_v2.carpools`, `carpool_v2.geo`, `carpool_v2.status`, `carpool_v2.requests` (payload
inclus), `operator_incentives` liés. La base opérationnelle est **réellement purgée**.

**Ce qui N'EST PAS supprimé :**

- `policy.incentives` — résultats d'incitation, pseudonymisés par `identity_key`, nécessaires
  aux appels de fonds / APDF. Rétention propre (financière), hors périmètre du job.
- Tout ce que le datalake a déjà matérialisé (`zone_trusted`, `aggregated/*`).

Le job est un batch idempotent (sélection par `start_datetime < now() - 15 mois`), à cadencer
(cron) et à journaliser (nb lignes supprimées par run).

## 6. Colonnes — ce qui disparaît à D

**Supprimées** (identifiants directs, aucun besoin > D) : `driver/passenger_phone`,
`_phone_trunc`, `_travelpass_name`, `_travelpass_user_id`, `licence_plate`,
`_operator_user_id`¹, `start_position` / `end_position` (GPS exact), `requests.payload`.

**Conservées côté datalake** (pseudonyme, matérialisées avant D) : `driver/passenger_identity_key`
(hash), `start/end_geo_code` (INSEE), `start/end_datetime`, `distance`, `seats`,
`operator_id/class`, `driver_revenue`, `passenger_contribution`, `passenger_over_18`, statuts.

¹ `operator_user_id` : son sort dépend de la décision export partenaires (§8).

## 7. Gates de validation (analyse code)

- ✅ **Recalcul incitations** — le moteur (`Policy.processStateless`, `TripRepositoryProvider.findTripByGeo`)
  ne lit que `identity_key` (hash) + INSEE geo_code + datetime + distance + seats + operator +
  montants + booléens. GPS exact lu par 2 helpers seulement (`isCloseTo`, `isStartAndEndInside`),
  1 seule campagne active (Rennes). Aucun besoin de PII brute. Couvert par la fenêtre chaude.
- ✅ **Fraude** — moteur en notebooks Python sur fenêtre glissante **24-48 h**, `fraud_status`
  figé en ~2 jours, jamais recalculé sur du vieux. Lookback analytique le plus profond (1 an,
  `simultaneous_group_trips`) sur geo_code + distance + statut, sans PII brute. `licence_plate`
  et `travelpass_user_id` ne servent à aucun détecteur. Suppression à D sans impact.
- ✅ **Légal / comptable** — pas d'obligation de conserver le payload brut (confirmé).
- ⏳ **Export partenaires** — décision ouverte (§8).
- ⚠️ **API opérateur** (`CarpoolLabelRepository`) — vérifier qu'aucun opérateur ne consulte le
  statut de ses trajets > D via l'API. Gate mineur, à confirmer.

## 8. Décisions ouvertes (hors-code)

1. **Export partenaires > D (AOM / juridique).** `export_partners` est aujourd'hui une VIEW qui
   relit le raw (`dlk_import.carpool_v2_carpools`) et émet `operator_user_id` (conducteur +
   passager), `identity_key` (hash) et coords GPS ~110 m–1 km, **sans borne d'ancienneté**.
   Options :
   - **(recommandé) Version pseudonymisée** : repointer sur `zone_trusted` (hash + H3 z8/z9 +
     INSEE). On perd `operator_user_id` et les coords fines. Le plus conforme.
   - **Rétention ciblée** : `zone_trusted` porte `operator_user_id` + coords coarsenées, avec TTL
     dur validé DPO. Réintroduit une donnée semi-identifiante longue mais bornée.
   - **Cap export à D** : interdire aux partenaires de tirer > D (borne min sur `start_at`).
     Changement produit/contractuel à valider avec les AOM.
   → À trancher côté métier ; conditionne le schéma de `zone_trusted` (cf. §9.2).
2. **Durée exacte + base légale de D.** Acter au **registre de traitement / AIPD** avec le DPO
   (le TODO existe déjà dans `20260703000000-datalake-fdw-export.sql`).

## 9. Pré-requis techniques (bloquants)

### 9.1 Full-refresh retiré des commandes

Un `dbt run --full-refresh` reconstruirait `zone_trusted` depuis le FDW → perte des lignes déjà
cooled. Mesure : `{{ config(full_refresh=false) }}` sur `zone_trusted.carpools` et **tous les
modèles à historique**. `--full-refresh` devient un **no-op** pour eux. Un vrai rebuild reste
possible mais uniquement en **tâche de maintenance manuelle délibérée**, jamais exposée comme
commande courante.

### 9.2 Étanchéité des zones — seul `trusted` lit le raw

Règle d'architecture : **`zone_trusted` est la seule zone autorisée à référencer le raw**
(`source('dlk_import', ...)` / FDW), en **incrémental-append**. Tout étage supérieur
(`aggregated`, `exposed`) lit `trusted` ou au-dessus, **jamais** plus bas.

Actions :

- Repointer `export_partners.sql` sur `ref('carpools')` (trusted) uniquement ; supprimer le join
  `sc = source('dlk_import','carpool_v2_carpools')`. Ce que l'export doit exposer (selon §8.1)
  doit alors être **porté par `zone_trusted`**.
- Auditer tous les modèles `aggregated/*` et `exposed/*` : aucune référence directe au raw.
- **Garde-fou CI** : un test/lint échoue si un modèle hors `zone_trusted` référence `dlk_import`
  (ou la source raw). Rend la règle exécutoire, pas seulement documentaire.

Mémo : *seul trusted peut append en incrémental depuis raw.*

## 10. Risques & points à challenger

- **Cap 6→12 mois de rétroactivité** : tenable côté produit ? Plus la fenêtre est large, plus le
  chaud grossit et plus la conformité RGPD se dégrade. 12 mois est un maximum, pas un confort.
- **Suppression totale vs anonymisation en place** : la suppression de ligne est plus propre
  RGPD mais irréversible ; valider qu'aucune exploitation ne dépend de la présence de la ligne
  (au-delà des gates §7).
- **Marge D − R = 3 mois** : suffisante face aux retards de batch / finalize ? À caler sur les
  délais réels observés.
- **`policy.incentives`** : sa propre durée de rétention (financière) doit être définie
  séparément — hors périmètre de ce design mais à ne pas oublier.

## 11. Suites

1. Trancher les décisions ouvertes §8 (DPO + AOM).
2. Confirmer D = 15 mois (§4).
3. Plan d'implémentation (skill writing-plans) : job de suppression, cap rétroactivité moteur,
   `full_refresh=false`, repointage `export_partners`, garde-fou CI, extension schéma `trusted`
   si rétention ciblée retenue.
