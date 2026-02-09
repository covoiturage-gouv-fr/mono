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

Copier `.env.example` vers `.env` et configurer :

| Variable                    | Description                              |
| --------------------------- | ---------------------------------------- |
| `APP_ENV`                   | Environnement (local, dev, staging, production) |
| `APP_API_URL`               | URL de l'API                             |
| `APP_APP_URL`               | URL du frontend                          |
| `APP_POSTGRES_URL`          | Connection string PostgreSQL             |
| `APP_REDIS_URL`             | Connection string Redis                  |
| `APP_JWT_SECRET`            | Clé secrète JWT                          |
| `APP_MAIL_SMTP_URL`         | URL serveur SMTP                         |
| `BREVO_API_KEY`             | Clé API Brevo                            |
| `BREVO_WELCOME_TEMPLATE_ID` | ID template email bienvenue              |
| `PROCONNECT_CLIENT_ID`      | Client ID ProConnect                     |
| `DEX_CLIENT_ID`             | Client ID Dex                            |

> **À compléter**

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
