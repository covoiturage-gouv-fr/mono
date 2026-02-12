# App Partners Architecture

Next.js 15 partner dashboard with React 19 and French Design System (DSFR).

## Project Structure

```
app-partners/src/
├── app/                    # App Router pages
│   ├── activite/           # Activity pages (campaigns, exports)
│   └── administration/     # Admin pages (users, operators, territories)
├── components/             # Reusable components
│   ├── auth/               # Authentication components
│   ├── campaign/           # Campaign-specific
│   ├── common/             # Shared UI (AlertMessage, Modal, Pagination)
│   ├── export/             # Export functionality
│   └── layout/             # Layout components
├── hooks/                  # Custom React hooks
├── interfaces/             # TypeScript interfaces
├── providers/              # Context providers
├── helpers/                # Utility functions
├── config/                 # Configuration
└── styles/                 # Global SCSS
```

## Commands

```bash
npm run dev     # Development on port 4200
npm run build   # Production build (static export)
npm run lint    # ESLint
```

## Key Patterns

### Routing (App Router)

- `/activite/campagnes` - Campaign management
- `/activite/export` - Data exports
- `/administration/profil` - User profile
- `/administration/utilisateurs` - User management
- `/administration/operateurs` - Operator management
- `/administration/territoires` - Territory management
- `/administration/cles-api` - API keys

### State Management

React Context for authentication:

```typescript
const { isAuth, user, logout } = useAuth();
```

No Redux/Zustand - lightweight context-based approach.

### API Integration

Custom `useRestQuery` hook:

```typescript
const { data, loading, error, refetch } = useRestQuery<DataType>(
  "v3",           // API version
  "endpoint/path",
  params,
  { method: "POST", paginate: false },
  [dependencies]
);
```

Available hooks:
- `useCampaignList()`, `useCampaignFind()`
- `useUsersList()`, `useOperatorsList()`, `useTerritoriesList()`
- `useExportsList()`, `useExportCreate()`, `useExportDownloadLink()`
- `useIncentiveGraph()`, `useOperatorsGraph()`

### DSFR Components

```typescript
import { Button, Input, Select, Table, Alert } from "@codegouvfr/react-dsfr";
import { fr } from "@codegouvfr/react-dsfr";

// Class utilities
className={fr.cx("fr-container", "fr-mb-7w")}
```

Key components: Header, Footer, Button, Input, Select, Table, Pagination, Alert, Badge, Download.

### Form Validation

Zod for schema validation:

```typescript
import { z } from "zod";

const schema = z.object({
  email: z.string().email(),
  name: z.string().min(1),
});
```

Input state for errors:
```typescript
<Input
  state={errors.email ? "error" : "default"}
  stateRelatedMessage={errors.email}
/>
```

### Modal Forms

`useActionsModal` hook for CRUD operations:

```typescript
const modal = useActionsModal<DataType>();
// modal.setCurrentRow(), modal.setOpenModal(), modal.setTypeModal()
```

### Authentication

ProConnect SSO flow:
1. Login via `ProConnectButton` -> ProConnect URL
2. Session check on load: `GET /auth/me`
3. Role-based access control

Role helpers:
```typescript
isRegistry(), isTerritory(), isOperator(), isAdmin(), isUser()
```

### Styling

1. **DSFR utilities**: `fr.cx()` for combining classes
2. **SCSS**: Global styles in `/src/styles/global.scss`
3. **Emotion**: Available for dynamic styling
4. **MUI**: Date pickers via `@mui/x-date-pickers`

### Analytics

Matomo integration:
```typescript
import { sendEvent } from "@socialgouv/matomo-next";
void sendEvent({ category: "campagne", action: "Consultation" });
```

## Configuration

Environment variables:
- `NEXT_PUBLIC_API_URL` - Backend API URL
- `NEXT_PUBLIC_API_REDIRECT` - Post-login redirect
- `NEXT_PUBLIC_PC_USER_URI` - ProConnect URI

Access via:
```typescript
Config.get<string>("auth.domain")
```

## Conventions

- Components: PascalCase (`AuthButton.tsx`)
- Hooks: camelCase with `use` prefix (`useCampaignList.ts`)
- Interfaces: PascalCase with `Interface` suffix
- Client components: Add `"use client"` directive

## Pre-build Hook

DSFR icon optimization runs before dev/build:
```bash
react-dsfr update-icons
```
