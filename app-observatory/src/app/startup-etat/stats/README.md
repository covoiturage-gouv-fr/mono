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
(périmètre national `code=XXXXX&type=country&indic=journeys`), comme les graphes de
`/observatoire/territoire` :

- **sans `month`** → série **annuelle**, remonte jusqu'à 2019 (historique complet).
  `TrajetsValidesTotal` en fait la somme = total « depuis 2019 » (aucun repli : le
  bloc n'est rendu qu'une fois l'API répondue) ; le graphe annuel l'affiche tel quel.
- **avec `month=1`** → série **mensuelle**, mais **~25 derniers mois seulement**.

Le repli figé de `data.ts` (`TRAJETS_PAR_MOIS`, `TRAJETS_PAR_AN`) est fusionné avec la
réponse API : il garde l'historique mensuel avant ~2024 et sert de secours graphe si
l'API est indisponible. `past` n'est **pas** un paramètre valide d'`evol-flux`
(renvoie HTTP 422).

### 2. Valeurs figées — `src/app/startup-etat/stats/data.ts`

Tout le reste (indicateurs chiffrés, coût unitaire, CEE courte / longue distance,
objectif 2027, repli des graphes de trajets) est **codé en dur** dans `data.ts`,
relevé à la main depuis le dashboard Metabase. Aucune dépendance externe, aucun
fichier à téléverser avant déploiement.

## Composants

Les blocs chiffrés réutilisent `components/observatoire/indicators/` (`Rows` +
`Indicator`, mêmes callouts à hauteur égale que l'Observatoire). `Indicator` a trois
props optionnelles ajoutées ici : `md` (`3` | `4` | `6`, largeur de colonne), `note`
(sous-texte gris, interligne resserré, comme l'ancien `fr-hint-text`) et `items`
(liste à puces sous le texte). Les indicateurs sont définis directement au type
`IndicatorProps` dans `data.ts` (objet `INDICATEURS`, `satisfies Record<string, IndicatorProps>`) ;
`page.tsx` ne fait que les regrouper par ligne dans `<Rows data={[…]}>`.

Les graphiques utilisent le composant commun `components/observatoire/charts/Chart.tsx`
(présentational : `kind` line/bar/doughnut, multi-séries, ligne d'objectif, datalabels,
bouton de téléchargement CSV, figcaption lecteur d'écran auto). `TrajetsChart`
(`components/vitrine/stats/`) est le seul wrapper propre à la vitrine : il fait le fetch
`evol-flux` + fusion avec le repli figé, puis délègue à `Chart`.

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
   coût unitaire = card 445, attestations FMD = card 417 (le sous-texte « dont … en
   2025 » est saisi à la main dans la `note`), plateformes actives = card 441,
   CEE courte distance = card 409, CEE longue distance = card 414 (dédoublonner le
   premier mois, présent 2× dans la source). Les autres entrées de `INDICATEURS`
   (`collectivites_accompagnees`, `pct_…`, `note_satisfaction_observatoire`,
   `telechargements_datagouv`, `campagnes_…`, `lignes_…`, `aires_…`) sont des textes
   éditoriaux figés dans le dashboard.

2. Mettre à jour les constantes de `src/app/startup-etat/stats/data.ts` (et la date du relevé
   dans son en-tête + le paragraphe de bas de page de `page.tsx`).

3. Redéployer l'observatoire. Les trajets par mois / an se rafraîchissent seuls via
   l'API.
