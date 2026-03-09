# Observatoire national du covoiturage au quotidien

L'Observatoire national du covoiturage au quotidien est une application web permettant de suivre l'évolution des pratiques de covoiturage courte distance en France et d'évaluer l'impact des politiques publiques mises en place par l'État, les collectivités et les entreprises.

**Site de production** : [observatoire.covoiturage.gouv.fr](https://observatoire.covoiturage.gouv.fr/)

## Objectifs

- Suivre l'évolution du covoiturage courte distance à l'échelle nationale et territoriale
- Évaluer l'efficacité des campagnes d'incitation financière
- Fournir des données ouvertes aux collectivités, entreprises et citoyens
- Accompagner l'objectif national de **3 millions de trajets quotidiens** en covoiturage d'ici 2027

## Stack technique

| Catégorie         | Technologie                                                                                      |
| ----------------- | ------------------------------------------------------------------------------------------------ |
| **Framework**     | Next.js 15.1 (App Router)                                                                        |
| **Langage**       | TypeScript 5.7, React 19                                                                         |
| **Design System** | [@codegouvfr/react-dsfr](https://github.com/codegouvfr/react-dsfr) (Système de Design de l'État) |
| **UI**            | Material-UI 6.4, Emotion CSS-in-JS, SASS                                                         |
| **Graphiques**    | Chart.js 4.4, react-chartjs-2                                                                    |
| **Cartographie**  | MapLibre GL 4.7, Deck.gl 9.1, react-map-gl                                                       |
| **Géospatial**    | H3-js (indexation hexagonale), Turf.js                                                           |
| **Contenu**       | MDX (next-mdx-remote), remark/rehype                                                             |
| **Recherche**     | Meilisearch 0.48                                                                                 |
| **Analytics**     | Matomo (@socialgouv/matomo-next)                                                                 |
| **Build**         | Export statique (`output: "export"`)                                                             |

## Structure du projet

```
src/
├── app/                    # Routes Next.js (App Router)
│   ├── page.tsx           # Page d'accueil
│   ├── observatoire/      # Tableaux de bord
│   │   ├── territoire/    # Données par territoire
│   │   └── campagnes-incitation/  # Suivi des incitations
│   ├── actualites/        # Section actualités
│   ├── ressources/        # Ressources documentaires
│   ├── collectivites/     # Pages pour collectivités
│   └── autres-acteurs/    # Employeurs, particuliers, plateformes
├── components/            # Composants React
│   ├── common/            # Composants réutilisables
│   ├── layout/            # Header, Footer, Navigation
│   └── observatoire/      # Composants du tableau de bord
├── config/                # Configuration (CMS, analytics, cartes)
├── context/               # Providers React Context
├── helpers/               # Fonctions utilitaires
├── hooks/                 # Custom hooks React
├── interfaces/            # Types TypeScript
└── styles/                # Styles globaux SCSS
```

## Fonctionnalités principales

### Tableau de bord territorial

- Analyse des données par périmètre : national, régional, départemental, communal
- Visualisation des flux de covoiturage sur carte interactive
- Graphiques d'évolution temporelle (mensuel, trimestriel, semestriel)
- Indicateurs clés : trajets, passagers, distances, taux d'occupation

### Cartographie avancée

- Cartes vectorielles MapLibre avec tuiles personnalisées
- Visualisation de densité hexagonale H3
- Flux origine-destination
- Aires de covoiturage

### Campagnes d'incitation

- Suivi des campagnes financières par territoire
- Montants distribués et trajets incités
- Évolution dans le temps

## Installation

```bash
# Installation des dépendances
npm install

# Lancement en développement
npm run dev

# Build de production (export statique)
npm run build
```

## Variables d'environnement

Copier `.env.example` vers `.env.local` et configurer :

```env
NEXT_PUBLIC_API_URL=           # URL de l'API backend
NEXT_PUBLIC_CMS_URL=           # URL du CMS Strapi
NEXT_PUBLIC_MAPTILER_KEY=      # Clé MapTiler pour les fonds de carte
NEXT_PUBLIC_MATOMO_URL=        # URL Matomo (analytics)
NEXT_PUBLIC_MATOMO_SITE_ID=    # ID du site Matomo
```

En environement de développement il est possible d'utiliser les endpoints de production suivants :

```env
NEXT_PUBLIC_API_URL=https://api.covoiturage.beta.gouv.fr/v3/observatory
NEXT_PUBLIC_CMS_URL=https://ncms.covoiturage.beta.gouv.fr
NEXT_PUBLIC_SEARCH_URL=https://search.covoiturage.beta.gouv.fr
```

Pour des raisons de sécurité les tokens devront être récuperés indépendamment

## Liens utiles

- [Registre de preuve de covoiturage](https://covoiturage.beta.gouv.fr/)
- [Plan national covoiturage](https://www.ecologie.gouv.fr/covoiturage-en-france-avantages-et-reglementationen-vigueur)
- [Système de Design de l'État (DSFR)](https://www.systeme-de-design.gouv.fr/)
- [beta.gouv.fr](https://beta.gouv.fr/)

## Licence

DINUM / DGITM / ADEME, 2017-2026

Ce projet est développé sous licence [Apache license 2.0](./LICENSE).
