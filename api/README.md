# API - Documentation Technique

Backend du Registre de Preuve de Covoiturage. API REST/RPC fournissant les services d'authentification, gestion des utilisateurs, territoires, opérateurs et exports.

## Ownership

| Rôle               | Personne | Contact      |
| ------------------- | -------- | ------------ |
| Tech Lead / Expert | [Nom]    | [Slack/Email] |
| Backup             | [Nom]    | [Slack/Email] |
| Product Owner      | [Nom]    | [Slack/Email] |

> **Note** : Le Tech Lead est la personne référente pour les questions techniques sur ce projet. Le Backup peut répondre en son absence.

## Stack Technique

| Élément         | Technologie                    |
| --------------- | ------------------------------ |
| Runtime         | Deno (TypeScript)              |
| Framework       | Express.js + ILOS (IoC custom) |
| Base de données | PostgreSQL                     |
| Cache           | Redis                          |
| Auth            | JWT, ProConnect, Dex (OAuth)   |
| Email           | Brevo API + Nodemailer         |
| Stockage        | S3 (Scaleway)                  |
| Monitoring      | Sentry, Prometheus             |

## Structure du Projet

```
src/
├── ilos/                     # Framework core (IoC, transports)
│   ├── core/                 # Kernel, decorators
│   ├── connection-postgres/  # DB connection
│   └── connection-redis/     # Cache connection
├── pdc/                      # Application principale
│   ├── proxy/                # HTTP transport, routing Express
│   │   ├── bootstrap.ts      # Point d'entrée
│   │   ├── HttpTransport.ts  # Middlewares & routes
│   │   └── Kernel.ts         # Composition services
│   ├── services/             # Modules métier
│   │   ├── auth/             # Authentification
│   │   ├── dashboard/        # Users, operators, territories
│   │   ├── export/           # Export données
│   │   ├── policy/           # Politiques incitation
│   │   ├── acquisition/      # Capture trajets
│   │   ├── operator/         # Gestion opérateurs
│   │   ├── territory/        # Gestion territoires
│   │   ├── apdf/             # Rapports APDF
│   │   └── honor/            # Génération certificats PDF
│   ├── providers/            # Utilitaires partagés
│   │   ├── notification/     # Mail transporter
│   │   ├── token/            # JWT provider
│   │   └── storage/          # S3 storage
│   └── middlewares/          # Express middlewares
├── db/                       # Base de données
│   ├── migrations/           # Migrations SQL
│   └── geo/                  # Données géographiques
└── lib/                      # Librairies partagées
```

## Installation & Lancement

### Prérequis

- Deno
- Docker & Docker Compose
- Just (task runner)

### Commandes

```bash
# Démarrer les containers (postgres, redis, mailer, s3)
just dc up

# Lancer les migrations
just migrate

# Seeder les données de test
just seed
just seed-local-users

# Lancer le serveur API
just serve              # Production
just watch              # Dev avec auto-reload

# Tests
just test-unit
just test-integration
just test-e2e

# Accès base de données
just db                 # Shell pgcli

# Arrêter les services
just stop
```

## Commandes API (dans le container)

L'API expose des commandes CLI accessibles via `just api <command>`. Pour lister toutes les commandes disponibles :

```bash
just api list
```

Commandes principales :

| Commande | Description |
| --- | --- |
| `just api http $PORT` | Démarrer le serveur HTTP |
| `just api export:create` | Créer une demande d'export |
| `just api export:process` | Traiter les exports en attente |
| `just api export:datagouv` | Exporter et uploader sur data.gouv.fr |
| `just api campaign:apply` | Appliquer les règles de campagne |
| `just api campaign:finalize` | Finaliser les règles stateful |
| `just api campaign:sync` | Synchroniser les sommes d'incitation |
| `just api campaign:stats` | Générer les stats de campagne |
| `just api apdf:export` | Exporter les APDF |
| `just api territory:index` | Indexer les territoires dans Meilisearch |
| `just api acquisition:geo` | Traiter le géocodage des acquisitions |
| `just api company:fetch <siret>` | Récupérer les données entreprise (INSEE SIRENE) |
| `just api journey:status <op_id> <journey_id>` | Vérifier le statut d'un trajet |
| `just api monitoring:stats:refresh` | Rafraîchir les vues matérialisées stats |

## Configuration

### Variables d'environnement

Copier `.env.example` vers `.env` et configurer les valeurs.

> Le fichier `.env` est ignoré par git et ne doit pas être commité.

#### Application

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `NODE_ENV` | Non | `local` | Environnement Node.js |
| `APP_ENV` | Non | `${NODE_ENV}` | Environnement applicatif (`local`, `dev`, `staging`, `production`). Contrôle la sécurité des cookies de session, l'accès au listing RPC, le filtrage des actions par environnement |
| `APP_MAINTENANCE` | Non | `false` | Active le mode maintenance (HTTP 503 sur toutes les requêtes) |
| `APP_VERSION` | Non | ISO timestamp | Version de l'application, utilisée dans les tags Sentry et les logs de démarrage |

#### URLs des services

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `APP_API_URL` | Non | `http://localhost:8080` | URL du backend REST/RPC |
| `APP_APP_URL` | Non | `http://localhost:4200` | URL du frontend. Utilisée pour le CORS et les redirections |
| `APP_CERT_URL` | Non | `http://localhost:4200` | URL du générateur d'attestations d'honneur. Utilisée pour le CORS sur `/honor` |
| `APP_SHOWCASE_URL` | Non | `http://localhost:1313` | URL du site vitrine. Utilisée pour le CORS sur `/contactform`, `/geo/search`, `/policy/simulate` |

#### Sécurité & Authentification

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `APP_JWT_SECRET` | **Oui** | — | Clé secrète pour signer les tokens JWT (HS256). Le changer invalide tous les tokens existants |
| `APP_RATE_LIMIT_MAX_FACTOR` | Non | `1` | Multiplicateur des limites de requêtes. `0` = désactivé |
| `APP_SENTRY_DSN` | Non | `""` | DSN privé Sentry pour le reporting d'erreurs. Vide = Sentry désactivé |
| `APP_SENTRY_ENV` | Non | `${NODE_ENV}` | Environnement Sentry pour filtrer les erreurs |

#### Base de données

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `APP_POSTGRES_URL` | **Oui** | `postgresql://postgres:postgres@postgres:5432/local` | Connection string PostgreSQL |
| `APP_REDIS_URL` | **Oui** | `redis://redis:6379` | Connection string Redis (sessions, rate limiting, cache) |

> TLS optionnel via `APP_POSTGRES_CA`, `APP_POSTGRES_CERT`, `APP_POSTGRES_KEY` (et équivalents `_PATH`).
> Idem pour Redis avec `APP_REDIS_CA`, `APP_REDIS_CERT`, `APP_REDIS_KEY`.

#### Email (SMTP / Nodemailer)

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `APP_MAIL_SMTP_URL` | **Oui** | — | URL du serveur SMTP (ex: `smtp://mailhog:1025`) |
| `APP_MAIL_FROM_NAME` | Non | `Preuve de covoiturage` | Nom de l'expéditeur |
| `APP_MAIL_FROM_EMAIL` | Non | `contact@covoiturage.beta.gouv.fr` | Email de l'expéditeur |
| `APP_MAIL_DEBUG_MODE` | Non | `false` | Redirige tous les emails sortants vers l'adresse de debug |
| `APP_MAIL_DEBUG_NAME` | Non | `Preuve de covoiturage` | Nom du destinataire de debug |
| `APP_MAIL_DEBUG_EMAIL` | Non | `contact@covoiturage.beta.gouv.fr` | Email du destinataire de debug |
| `APP_MAIL_VERIFY_SMTP` | Non | `false` | Vérifie la connexion SMTP au boot (exit code 1 si échec) |

#### Email (Brevo)

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `BREVO_API_KEY` | Non | `""` | Clé API Brevo. Vide = service désactivé |
| `BREVO_WELCOME_TEMPLATE_ID` | Non | `0` | ID du template Brevo pour l'email de bienvenue. `0` = désactivé |
| `BREVO_API_URL` | Non | `https://api.brevo.com/v3/smtp/email` | Endpoint API Brevo |

#### Stockage S3

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `AWS_ACCESS_KEY_ID` | **Oui** | — | Clé d'accès S3 |
| `AWS_SECRET_ACCESS_KEY` | **Oui** | — | Clé secrète S3 |
| `AWS_ENDPOINT` | Non | `https://s3.fr-par.scw.cloud` | Endpoint S3 (Scaleway, Minio, AWS…) |
| `AWS_REGION` | Non | `fr-par` | Région S3 |

> Préfixe de bucket configurable via `AWS_BUCKET_PREFIX`. Override par bucket possible via `AWS_BUCKET_{NAME}_ENDPOINT`.
> Buckets utilisés : `APDF`, `Export`, `Public`, `GeoDatasetsMirror`.

#### Cache de routes (proxy Redis)

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `APP_ROUTECACHE_ENABLED` | Non | `true` | Active le cache de réponses HTTP dans Redis |
| `APP_ROUTECACHE_GZIP_ENABLED` | Non | `true` | Active la compression GZip des payloads en cache |
| `APP_ROUTECACHE_AUTHTOKEN` | Non | — | Token `[a-zA-Z0-9]` pour le flush du cache via `DELETE /cache?prefix=` (header `X-Route-Cache-Auth`) |

#### Meilisearch

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `APP_MEILISEARCH_HOST` | Non | `http://localhost` | URL du serveur Meilisearch |
| `APP_MEILISEARCH_APIKEY` | Non | `""` | Clé API Meilisearch |
| `APP_MEILISEARCH_INDEX` | Non | `geo` | Nom de l'index pour la recherche de territoires |

#### Export

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `APP_EXPORT_MIN_START` | Non | `63072000000` (3 ans) | Ancienneté max des données exportables (ms depuis maintenant) |
| `APP_EXPORT_MAX_END` | Non | `432000000` (5 jours) | Délai min avant export des données récentes (ms depuis maintenant) |

#### ProConnect (SSO gouvernemental)

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `PROCONNECT_CLIENT_ID` | **Oui** | — | Client ID OAuth2/OIDC ProConnect |
| `PROCONNECT_CLIENT_SECRET` | **Oui** | — | Client secret OAuth2/OIDC ProConnect |
| `PROCONNECT_BASE_URL` | **Oui** | — | URL de découverte OIDC ProConnect |
| `PROCONNECT_REDIRECT_URL` | **Oui** | — | URL de callback après authentification |
| `PROCONNECT_LOGOUT_REDIRECT_URL` | **Oui** | — | URL de redirection après déconnexion |

#### Dex (authentification opérateurs)

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `DEX_CLIENT_ID` | **Oui** | — | Client ID OAuth2 Dex |
| `DEX_CLIENT_SECRET` | **Oui** | — | Client secret OAuth2 Dex |
| `DEX_BASE_URL` | **Oui** | — | URL du serveur Dex pour la découverte OIDC |
| `DEX_GRPC_HOST` | **Oui** | — | Hostname du serveur gRPC Dex |
| `DEX_GRPC_PORT` | Non | `5557` | Port du serveur gRPC Dex |

#### Contact

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `APP_CONTACTFORM_TO` | Non | — | Adresse email destinataire du formulaire de contact du site vitrine |

#### Tests E2E

| Variable | Requis | Défaut | Description |
| --- | --- | --- | --- |
| `APIE2E_API_URL` | Non | `http://localhost:8080` | URL de l'API pour les tests E2E |
| `APIE2E_AUTH_ADMIN_EMAIL` | Non | `admin@example.com` | Email du compte admin de test |
| `APIE2E_AUTH_ADMIN_PASSWORD` | Non | `admin1234` | Mot de passe du compte admin de test |
| `APIE2E_AUTH_OPERATOR_EMAIL` | Non | `operator@example.com` | Email du compte opérateur de test |
| `APIE2E_AUTH_OPERATOR_PASSWORD` | Non | `admin1234` | Mot de passe du compte opérateur de test |
| `APIE2E_AUTH_TERRITORY_EMAIL` | Non | `territory@example.com` | Email du compte territoire de test |
| `APIE2E_AUTH_TERRITORY_PASSWORD` | Non | `admin1234` | Mot de passe du compte territoire de test |

## Architecture API

### Patterns

- **Dependency Injection** : Conteneur IoC (Inversify)
- **RPC + REST Hybride** : JSON-RPC interne, REST externe
- **Service Provider Pattern** : Chaque service = provider + actions + repositories
- **Middleware Chain** : Validation permissions, transformation données

### Format des endpoints

**REST (externe)** : `/v3/{service}/{action}`

```
GET  /v3/dashboard/users
POST /v3/dashboard/user
```

**RPC (interne)** : `POST /rpc`

```json
{
  "method": "dashboard:createUser",
  "params": { ... }
}
```

## Sécurité

- JWT pour authentification
- CORS configuré par route
- Rate limiting (global + spécifique auth)
- Helmet (headers HTTP)
- Protection XSS

## Fichiers clés

| Fichier                                  | Description                  |
| ---------------------------------------- | ---------------------------- |
| `src/pdc/proxy/HttpTransport.ts`         | Routing & middlewares HTTP   |
| `src/pdc/proxy/Kernel.ts`                | Registration des services    |
| `src/pdc/services/dashboard/actions/`    | Actions CRUD utilisateurs    |
| `src/pdc/providers/notification/`        | Envoi emails                 |
| `justfile`                               | Commandes de développement   |
