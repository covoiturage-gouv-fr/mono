# App Observatory Architecture

Next.js 15 public statistics dashboard with maps and data visualization.

## Project Structure

```
app-observatory/src/
├── app/                    # App Router pages
│   ├── observatoire/       # Observatory dashboard pages
│   ├── actualites/         # News/articles
│   └── ressources/         # Resources and documentation
├── components/             # 70+ components by domain
│   ├── observatoire/       # Dashboard components
│   ├── actualites/         # News components
│   ├── ressources/         # Resource components
│   └── common/             # Shared UI
├── hooks/                  # Data fetching hooks
├── interfaces/             # TypeScript interfaces
└── styles/                 # Global SCSS
```

## Commands

```bash
npm run dev     # Development on port 4200
npm run build   # Production build
npm run lint    # ESLint
```

## Key Technologies

| Library | Purpose |
|---------|---------|
| MapLibre GL | Base map rendering |
| Deck.gl | High-performance layers (H3, Arcs) |
| react-map-gl | React wrapper for MapLibre |
| Chart.js | Line and distribution charts |
| H3-JS | Hexagonal spatial indexing |
| Meilisearch | Full-text search |
| next-mdx-remote | MDX content rendering |

## Map Integration

### Base Map Component

```typescript
import Map from "react-map-gl/maplibre";
import DeckGL from "@deck.gl/react";
```

### Deck.gl Layers

- `H3HexagonLayer` - Density visualization with hexagonal bins
- `ArcLayer` - Origin-destination flow visualization

### H3 Hexagonal Binning

```typescript
import { cellToBoundary, cellToLatLng } from "h3-js";

// Resolution 9 for journey data
const h3Index = latLngToCell(lat, lng, 9);
```

## Data Visualization

Chart.js with react-chartjs-2:

```typescript
import { Line } from "react-chartjs-2";
import { Chart, CategoryScale, LinearScale, PointElement, LineElement } from "chart.js";

Chart.register(CategoryScale, LinearScale, PointElement, LineElement);
```

## API Data Fetching

Custom hooks for API calls:

```typescript
const { data, loading, error } = useApi<DataType>(url, params);
const jsonData = useJson<DataType>(url);
```

### Key Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/observatory/journeys` | Journey statistics |
| `/observatory/directions` | Direction analysis |
| `/observatory/od` | Origin-destination matrices |
| `/observatory/territories` | Territory data |

## Meilisearch Integration

Multi-index search:

```typescript
import { MeiliSearch } from "meilisearch";

const client = new MeiliSearch({ host: MEILISEARCH_URL });
const results = await client.multiSearch({
  queries: [
    { indexUid: "article", q: query },
    { indexUid: "resource", q: query },
    { indexUid: "page", q: query },
  ],
});
```

## MDX Content

CMS content via next-mdx-remote:

```typescript
import { MDXRemote } from "next-mdx-remote/rsc";
import remarkGfm from "remark-gfm";
import rehypeSlug from "rehype-slug";

<MDXRemote
  source={content}
  options={{ mdxOptions: { remarkPlugins: [remarkGfm], rehypePlugins: [rehypeSlug] }}}
/>
```

Content fetched from Strapi CMS with 60s revalidation.

## Geographic Data

### Type Definitions

```typescript
type PerimeterType = "country" | "region" | "department" | "aom" | "epci" | "commune";
type INSEECode = string; // 5-digit commune code
```

### Classification

Jenks natural breaks for choropleth maps:

```typescript
import { jenks } from "simple-statistics";
const breaks = jenks(values, 5);
```

### GeoJSON Pattern

```typescript
interface FeatureCollection {
  type: "FeatureCollection";
  features: Feature[];
}
```

## Styling

1. **DSFR**: French design system components
2. **MUI**: Material UI with DSFR theming
3. **SCSS**: Global styles and CSS modules
4. **Emotion**: CSS-in-JS for dynamic styling

## Pre-build Hook

Icon optimization:

```bash
only-include-used-icons
```

## Environment Variables

- `NEXT_PUBLIC_API_URL` - Backend API
- `NEXT_PUBLIC_MEILISEARCH_URL` - Search engine
- `NEXT_PUBLIC_MAPBOX_TOKEN` - Map tiles (if used)

## Conventions

- Suspense boundaries for async data
- Server/client component split
- Container/presentational pattern
- Strong TypeScript typing for geographic data
