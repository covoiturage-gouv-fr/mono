# Page « Mesures d'impact de la Startup d'État »

Reprise DSFR du dashboard Metabase 111
(`https://stats.covoiturage.beta.gouv.fr/dashboard/111`,
public : `https://stats.covoiturage.beta.gouv.fr/public/dashboard/2084d346-8e3b-495e-9b10-b4870a35632a`).

Route Next : `src/app/startup-etat/stats/` → `/startup-etat/stats`. Le micro-site
vitrine `covoiturage.beta.gouv.fr` est servi depuis `out/startup-etat/` : la page est
donc publiée à **`covoiturage.beta.gouv.fr/stats`** (elle remplace l'ancienne
redirection 301 `/statistiques` → Metabase, à retirer côté hébergement). Elle est
aussi accessible via `observatoire.covoiturage.gouv.fr/startup-etat/stats`.

Chrome hérité de `src/app/startup-etat/layout.tsx` (`VitrineHeader` / `VitrineFooter`).
Lien d'accès : bouton « Notre impact » du `VitrineHeader` et du `VitrineFooter`.

## Sources de données

### 1. API de l'Observatoire (datalake) — au runtime

`TrajetsChart` et `TrajetsValidesTotal` appellent `OBSERVATORY_API_URL/evol-flux`
(périmètre national `code=XXXXX&type=country`, `indic=journeys`, `past=7`), comme les
graphes de `/observatoire/territoire`. La série est complétée / doublée par un repli
figé (`data.ts`) : historique complet depuis 2019 et affichage garanti si l'API est
indisponible.

### 2. Valeurs figées — `src/app/startup-etat/stats/data.ts`

Tout le reste (indicateurs chiffrés, coût unitaire, CEE courte / longue distance,
objectif 2027, total de repli) est **codé en dur** dans `data.ts`, relevé à la main
depuis le dashboard Metabase. Aucune dépendance externe, aucun fichier à téléverser
avant déploiement.

## Rafraîchir les données

1. Les cartes du dashboard public sont interrogeables sans authentification :

   ```bash
   U=2084d346-8e3b-495e-9b10-b4870a35632a
   # structure du dashboard (liste des cartes + textes)
   curl -s "https://stats.covoiturage.beta.gouv.fr/api/public/dashboard/$U"
   # données d'une carte (dashcardId / cardId lus dans la réponse ci-dessus)
   curl -s "https://stats.covoiturage.beta.gouv.fr/api/public/dashboard/$U/dashcard/<dashcardId>/card/<cardId>"
   ```

   Correspondances : trajets/mois = card 413, trajets/an = card 396,
   coût unitaire = card 445, attestations FMD = card 417, plateformes actives = card 441,
   CEE courte distance = card 409, CEE longue distance = card 414 (dédoublonner le
   premier mois, présent 2× dans la source). Les autres indicateurs
   (`collectivites_accompagnees`, `pct_…`, `note_satisfaction_observatoire`,
   `telechargements_datagouv`, `campagnes_…`, `lignes_…`, `aires_…`) sont des textes
   éditoriaux figés dans le dashboard.

2. Mettre à jour les constantes de `src/app/startup-etat/stats/data.ts` (et la date du relevé
   dans son en-tête + le paragraphe de bas de page de `page.tsx`).

3. Redéployer l'observatoire. Les trajets par mois / an se rafraîchissent seuls via
   l'API.
