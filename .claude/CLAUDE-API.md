# CLAUDE - API INSTRUCTIONS

## Essential Commands

All commands run from `api/` directory using **Just** task runner.

### Docker Stack

```bash
just dev                    # Start services (postgres, redis, s3, mailer, dex)
just proxy                  # Start with Traefik on *.covoiturage.test
just stop                   # Stop services
just logs [service] [-f]    # View logs
just build                  # Build containers
```

### Database

```bash
just migrate                # Run schema migrations
just seed                   # Seed test data
just seed-local-users       # Add test users (admin@example.com / admin1234)
just source                 # Load geographic perimeter data
just db                     # Connect with pgcli
just drop_test_databases    # Clean test_* databases
```

### API Development

```bash
just serve                  # Start API on $PORT
just watch                  # Dev mode with auto-reload
just debug                  # Deno REPL with app kernel loaded
just api <command>          # Run API commands (e.g., export:create)
just env                    # Show APP_* environment variables
```

### Testing

```bash
just test [pattern]              # Run all tests matching pattern
just test-unit                   # Unit tests only (parallel, no services needed)
just test-integration            # Integration tests (requires running stack)
just test-e2e                    # E2E tests
just ci_test_unit                # CI unit test pipeline
just ci_test_integration         # CI integration test pipeline (full stack)
```

### Deno

```bash
just cache                  # Cache dependencies for main.ts
just lock                   # Update deno.lock
```

### Frontend Apps

```bash
# app-partners or app-observatory
npm run dev                 # Development server on port 4200
npm run build               # Production build
npm run lint                # ESLint

# app-attestation
npm start                   # Development server
npm run build               # Production build
npm test                    # Jasmine/Karma tests
```

## API Architecture

### ILOS Framework

The API uses a custom IoC framework (ILOS) with Inversify for dependency injection.

**Key Decorators:**

| Decorator | Purpose |
|-----------|---------|
| `@handler()` | Defines action handlers with service, method, middlewares, API routes |
| `@serviceProvider()` | Decorates service providers with handlers, commands, validators |
| `@provider()` | Marks injectable service classes |
| `@middleware()` | Marks middleware classes |
| `@command()` | CLI commands with signature and options |

**Key Components:**

- **Kernel** (`api/src/pdc/proxy/Kernel.ts`): Registers all service providers, connections, and commands
- **Service Providers**: Each domain module is a service provider that registers actions and repositories
- **Actions**: Extend `Action` class, implement `handle(params, context)` method
- **Repositories**: Database access layer using PostgreSQL

### Service Provider Structure

Each service follows this pattern:

```
services/<name>/
├── <Name>ServiceProvider.ts   # Registers all components
├── actions/                   # Action handlers
├── repositories/              # Database access
├── contracts/                 # TypeScript interfaces
├── commands/                  # CLI commands (optional)
└── config/                    # Service configuration
```

### Action Pattern

```typescript
@handler({
  service: "acquisition",
  method: "create",
  middlewares: [
    ["validate", CreateJourneyParamsValidator],
    "scopeToSelf",
  ],
  apiRoute: {
    path: "/v3/journeys",
    method: "POST",
    rateLimiter: { max: 2000 },
  },
})
export class CreateJourneyAction extends Action {
  async handle(params: ParamsType, context: ContextType): Promise<ResultType> {
    // Implementation
  }
}
```

### Repository Pattern

Use the `sql` template literal for parameterized queries:

```typescript
import { sql } from "@/lib/pg/sql.ts";

const result = await this.connection.getClient().query(sql`
  SELECT * FROM carpools WHERE id = ${id}
`);
```

### Service Modules

Located in `api/src/pdc/services/`:

| Service | Purpose |
|---------|---------|
| `acquisition` | Trip data capture from operators |
| `auth` | Authentication (JWT, ProConnect, Dex) |
| `dashboard` | CRUD for users, operators, territories for `app-partners` |
| `export` | Data export functionality |
| `policy` | Carpooling campaigns |
| `operator` | Operator management |
| `territory` | Territory/jurisdiction management |
| `apdf` | APDF reporting |
| `cee` | Mobility tax incentive (CEE) |
| `honor` | PDF certificate generation |
| `observatory` | Public statistics APIs |
| `geo` | Geolocation services |
| `company` | Company lookup (INSEE API) |

### API Routing

- External REST: `GET/POST/PUT/DELETE /v3/{service}/{action}`
- Internal RPC: `POST /rpc` with `{ "method": "service:action", "params": {...} }`

Routes defined in `api/src/pdc/proxy/HttpTransport.ts`

#### Notes

- Internal RPC calls will be migrated to shared providers in `api/src/pdc/providers`
- External routes should be defined in the Action decorator

### Shared Types

Domain interfaces in `shared/` directory are used across API and frontends. Import with `@pdc/shared/{domain}`.

#### Notes

- Shared interfaces will be deprecated as the older frontend has been removed in favor of app-partners.

## Database

PostgreSQL 16 with PostGIS extension. Migrations in `api/src/db/migrations/`.

Key schemas: users, operators, territories, carpools, policies, exports, fraud_checks, observatory views, CEE tables.

`DenoPostgresConnection.ts` is the new provider and should replace `LegacyPostgresConnection.ts`

The `sql` template string must be used for all queries.

No ORM is used in the API.

Get a configured `pgcli`  running `just db`

## Configuration

### Environment Variables

Copy `api/.env.example` to `api/.env`. Key variables:

- `APP_POSTGRES_URL` - PostgreSQL connection
- `APP_REDIS_URL` - Redis connection
- `APP_MAIL_SMTP_URL` - SMTP for emails
- `AWS_*` - S3/MinIO storage configuration
- `DEX_BASE_URL` - OAuth provider URL

### Deno Configuration

`api/deno.jsonc`:

- Import aliases: `@/` maps to `./src/`  
  (use Deno's _Organise Imports_ LSP feature as VSCode's one rewrites ugly `../../../`)
- Legacy decorators enabled for Inversify
- Line width: 120 for formatting

## Docker Compose Overlays

- `docker-compose.base.yml` - Service definitions (no exposed ports)
- `docker-compose.dev.yml` - Exposes ports for localhost development (default `just dc`)
- `docker-compose.proxy.yml` - Adds Traefik for *.covoiturage.test domains
- `docker-compose.e2e.yml` - E2E test configuration (`just dc_e2e`)

Run `just add-hosts` to add domain aliases to /etc/hosts
or configure a local of `*.test` to `localhost` ;)

## Pre-commit Hooks

Talisman for secret detection configured in `.talismanrc`.
Run `pre-commit install` when `talisman` hooks is not found.

## Development Notes

- NixOS users: Add `DOCKER_SOCK=/run/user/1000/docker.sock` to `api/.env`
- Use `just seed-local-users` for test accounts (requires `APP_ENV=local`)
- Keep test databases with `APP_POSTGRES_KEEP_TEST_DATABASES=true`, then clean with `just drop_test_databases`
