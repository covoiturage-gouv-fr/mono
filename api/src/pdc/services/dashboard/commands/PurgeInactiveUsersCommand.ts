import { command, CommandInterface, ConfigInterfaceResolver } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import {
  MailTemplateNotificationInterface,
  NotificationTransporterInterfaceResolver,
} from "@/pdc/providers/notification/index.ts";
import { UsersRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts";
import { InactiveUserDeletionWarningNotification } from "../notifications/InactiveUserDeletionWarningNotification.ts";

interface Options {
  inactivity: string;
  grace: string;
  appUrl: string;
  dryRun: boolean;
}

// Garde-fou : le transporter mail no-op silencieusement si non configuré.
// On refuse de tourner sans SMTP, sinon on marquerait/supprimerait des comptes jamais avertis.
export function assertNotificationConfigured(config: ConfigInterfaceResolver): void {
  const smtp = config.get("notification.mail.smtp", null);
  if (!smtp) {
    throw new Error(
      "[purge-inactive-users] notification.mail.smtp non configuré ; arrêt pour éviter de supprimer des comptes non avertis",
    );
  }
}

@command({
  signature: "dashboard:purge-inactive-users",
  description: "Avertit puis supprime les comptes back-office inactifs (RGPD)",
  options: [
    { signature: "--inactivity <inactivity>", description: "Seuil d'inactivité (interval PG)", default: "12 months" },
    { signature: "--grace <grace>", description: "Durée du préavis (interval PG)", default: "7 days" },
    {
      signature: "--app-url <appUrl>",
      description: "URL de reconnexion (espace partenaire)",
      default: "https://partenaire.covoiturage.beta.gouv.fr",
    },
    { signature: "--dry-run", description: "N'envoie rien et ne supprime rien, log seulement" },
  ],
})
export class PurgeInactiveUsersCommand implements CommandInterface {
  constructor(
    private config: ConfigInterfaceResolver,
    private usersRepository: UsersRepositoryInterfaceResolver,
    private emailer: NotificationTransporterInterfaceResolver<MailTemplateNotificationInterface>,
  ) {}

  public async call(options: Options): Promise<string> {
    assertNotificationConfigured(this.config);

    // Phase 1 — avertir
    const toWarn = await this.usersRepository.findUsersToWarn(options.inactivity);
    let warned = 0;
    for (const user of toWarn) {
      const fullname = `${user.firstname ?? ""} ${user.lastname ?? ""}`.trim() || user.email;
      try {
        if (options.dryRun) {
          logger.info(`[purge-inactive-users] (dry-run) avertirait ${user._id}`);
        } else {
          await this.emailer.send(
            new InactiveUserDeletionWarningNotification(`${fullname} <${user.email}>`, {
              fullname,
              action_href: options.appUrl,
            }),
          );
          // Si markUserWarned throw ici, warned peut sous-compter (sens sûr : l'utilisateur sera réavertit, jamais supprimé à tort).
          await this.usersRepository.markUserWarned(user._id);
          warned++;
        }
      } catch (e) {
        logger.error(`[purge-inactive-users] échec avertissement ${user._id}: ${e instanceof Error ? e.message : e}`);
      }
    }

    // Phase 2 — supprimer
    // Suppression atomique (DELETE ... RETURNING avec les mêmes filtres) pour éviter
    // toute course entre une sélection et une suppression : un compte reconnecté pendant
    // le run n'est pas supprimé. En dry-run on se contente de lister les candidats.
    let candidates = 0;
    let deleted = 0;
    if (options.dryRun) {
      const toDelete = await this.usersRepository.findUsersToDelete(options.grace);
      candidates = toDelete.length;
      for (const user of toDelete) {
        logger.info(`[purge-inactive-users] (dry-run) supprimerait ${user._id}`);
      }
    } else {
      const removed = await this.usersRepository.deleteInactiveUsers(options.grace);
      deleted = removed.length;
      candidates = removed.length;
    }

    const summary = `[purge-inactive-users] avertis=${warned}/${toWarn.length} supprimés=${deleted}/${candidates}` +
      (options.dryRun ? " (dry-run)" : "");
    logger.info(summary);
    return summary;
  }
}
