# Suppression automatique des comptes back-office inactifs (RGPD)

Date : 2026-07-16
Statut : design validé, à implémenter

## Objectif

Automatiser une obligation RGPD (limitation de conservation, art. 5.1.e) :
supprimer les comptes back-office `auth.users` inactifs depuis plus de 12 mois,
après un préavis d'une semaine leur laissant la possibilité d'annuler en se
reconnectant.

Remplace le process manuel actuel (export Metabase → mail Brevo à la liste →
reminder manuel une semaine après → diff des deux exports → suppression).

## Ce que le design tranche (et pourquoi)

- **Le CSV manuel encode déjà un état de contact persistant.** On le déplace en
  base : une colonne `deletion_warned_at`. Ça supprime l'export, le reminder, le
  diff, et rend la détection de reconnexion triviale (comparaison de deux dates).
- **Pas de table d'audit / pas d'anonymisation.** Le mail de préavis n'est pas
  une obligation légale (c'est une courtoisie produit), donc rien à *prouver* sur
  son envoi. Un journal de suppression conservant email/nom recréerait la donnée
  perso qu'on est censé effacer → contre-productif RGPD. L'accountability se prouve
  par la politique de rétention documentée + le code + les logs de run. La preuve
  support (« vous m'avez viré ! ») = les logs du relais SMTP.
- **Un seul command idempotent lancé chaque semaine**, plutôt qu'un cron mensuel
  dérivé sans marqueur (qui pourrait supprimer un inactif jamais averti si son
  anniversaire d'inactivité tombe entre deux runs).

## Périmètre

- **Cibles** : comptes `role` en `operator.*` et `territory.*`.
- **Exclus** : `registry.*` (équipe RPC interne / support), jamais auto-supprimés.
  Filtre : `role NOT LIKE 'registry.%'`.
- `auth.users` = comptes back-office uniquement ; la suppression ne touche pas les
  données de trajets/covoitureurs.

## Schéma

Migration `api/src/db/migrations/<ts>-add-deletion-warned-at-to-users.sql` :

```sql
ALTER TABLE auth.users ADD COLUMN deletion_warned_at timestamptz;
```

- Nullable. `NULL` = pas dans un cycle de suppression.
- `last_login_at` est `NOT NULL DEFAULT now()` : un compte jamais connecté a
  `last_login_at = created_at`, donc pas d'edge-case NULL à gérer.

## Reset sur reconnexion

Dans `auth/providers/UserRepository.ts::authenticateByEmail` (seul writer de
`last_login_at`), remettre le marqueur à NULL en même temps :

```sql
UPDATE auth.users SET last_login_at = now(), deletion_warned_at = NULL WHERE _id = $1
```

Se reconnecter annule le process et rend le compte ré-éligible à un futur cycle.

## Command

Nouveau `@command` dans le service `dashboard` (là où vivent `UsersRepository` et
les users), signature `dashboard:purge-inactive-users`, sur le modèle de
`apdf/commands/ExportCommand.ts`.

Options CLI :

- `--inactivity` (défaut `12 months`) — seuil d'inactivité.
- `--grace` (défaut `7 days`) — durée du préavis.
- `--dry-run` — log les cibles sans envoyer ni supprimer.

Deux phases par run, dans cet ordre :

### Phase 1 — Avertir

```sql
SELECT _id, email, firstname, lastname
FROM auth.users
WHERE last_login_at < now() - :inactivity
  AND deletion_warned_at IS NULL
  AND role NOT LIKE 'registry.%'
```

Pour chaque user, dans un `try/catch` individuel :

1. `emailer.send(new InactiveUserDeletionWarningNotification(...))`.
2. Si l'envoi a réellement eu lieu → `UPDATE auth.users SET deletion_warned_at = now() WHERE _id = $1`.

**Garde importante** : `NotificationMailTransporter.send()` no-op silencieusement
si le transporter est null (`this.transporter && ...`). Ne poser le marqueur
qu'après un envoi confirmé, sinon on supprimerait un compte jamais réellement
averti. Un `send()` qui throw (erreur SMTP) → pas de marquage → repris au run
suivant.

### Phase 2 — Supprimer

```sql
SELECT _id FROM auth.users
WHERE deletion_warned_at < now() - :grace
  AND last_login_at < deletion_warned_at   -- pas reconnecté depuis l'avertissement
  AND role NOT LIKE 'registry.%'
```

→ hard `DELETE` via `UsersRepository.deleteUser` existant (cascade FK).

Avec cadence hebdo + grace 7j, un user averti au run N est supprimé au run N+1.
Note : si le scheduler tire un run un peu avant les 7j pleins (drift), la
suppression glisse au run suivant (une semaine de plus). Comportement acceptable
— le préavis promis reste « au moins 7 jours ».

## Email

Notification `InactiveUserDeletionWarningNotification extends DefaultNotification`
dans `dashboard/notifications/`, sur le modèle de
`export/notifications/ExportCSVNotification.ts`.

- `subject` (à confirmer) : « Votre compte Registre de preuve de covoiturage va être supprimé ».
- `action_message` (texte du bouton) : « Je me connecte au RPC ».
- `action_href` = URL de connexion à l'espace partenaire.
- Contact affiché : contact@covoiturage.beta.gouv.fr.

Corps (contenu déjà utilisé aujourd'hui, à porter en MJML/texte) :

> Bonjour,
>
> Nous avons remarqué que vous n'avez pas utilisé votre compte Registre de preuve
> de covoiturage depuis plus d'un an. Pour rappel, celui-ci vous donne accès à
> votre espace partenaire pour effectuer des exports de données ou consulter une
> campagne d'incitations financières.
>
> Conformément à notre politique de confidentialité et aux exigences du Règlement
> Général sur la Protection des Données (RGPD), nous tenons à vous informer que
> votre compte sera automatiquement supprimé dans 7 jours si aucune activité n'est
> détectée d'ici là.
>
> Si vous souhaitez conserver votre compte et l'accès à votre espace partenaire,
> reconnectez-vous en cliquant sur le bouton ci-dessous. [Je me connecte au RPC]
>
> En cas de suppression, toutes vos données personnelles seront définitivement
> effacées du Registre de preuve de covoiturage. Vous ne serez pas supprimé des
> newsletters auxquelles vous avez pu vous inscrire.
>
> Si vous avez des questions ou besoin d'assistance, n'hésitez pas à nous
> contacter à l'adresse contact@covoiturage.beta.gouv.fr.
>
> Nous vous remercions de votre compréhension […]. Cordialement,

Envoi via `NotificationTransporterInterfaceResolver` (transporter nodemailer/MJML
des exports), injecté dans le command.

**Click ≠ login** : l'annulation se déclenche sur un login réussi
(`authenticateByEmail`), pas sur le clic du bouton. Le texte dit donc
« reconnectez-vous » (et non « confirmez votre intention ») : le bouton mène à la
page de login, et seule l'authentification aboutie remet le marqueur à NULL.

## Trigger

Ajouter un **CronJob k8s hebdomadaire** lançant
`deno run src/main.ts dashboard:purge-inactive-users` (hébergement = cluster k8s,
pas de PaaS). Aucun scheduler in-app dans le repo aujourd'hui ; le déclencheur se
configure côté manifests k8s.

## Robustesse

- **Idempotent** : le marqueur empêche de ré-emailer dans le cycle ; un run
  interrompu/partiel est repris au run suivant (les non-marqués restent candidats).
- **Dry-run** obligatoire pour valider le périmètre avant premier run réel.
- **Pas de PII conservée** hors des comptes eux-mêmes.

## Tests

- Unitaire : logique de sélection des cibles (seuils, exclusion `registry.*`,
  reconnexion `last_login_at < deletion_warned_at`).
- Intégration (repo) : requêtes phase 1 / phase 2 sur une base de test, reset du
  marqueur sur `authenticateByEmail`, garde « pas de marquage si envoi non
  confirmé ».

## Hors-scope

- Anonymisation / soft-delete (on garde le hard DELETE existant).
- Table d'audit de suppression.
- Notification aux covoitureurs (ce process ne concerne que `auth.users`).
