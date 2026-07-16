import { DefaultNotification, DefaultTemplateData } from "@/pdc/providers/notification/index.ts";

export interface InactiveUserDeletionWarningTemplateData extends DefaultTemplateData {
  action_href: string;
}

const message_html = `
<p>Nous avons remarqué que vous n'avez pas utilisé votre compte Registre de preuve de covoiturage depuis plus d'un an. Pour rappel, celui-ci vous donne accès à votre espace partenaire pour effectuer des exports de données ou consulter une campagne d'incitations financières.</p>
<p>Conformément à notre politique de confidentialité et aux exigences du Règlement Général sur la Protection des Données (RGPD), nous tenons à vous informer que votre compte sera automatiquement supprimé dans 7 jours si aucune activité n'est détectée d'ici là.</p>
<p>Si vous souhaitez conserver votre compte et l'accès à votre espace partenaire, reconnectez-vous en cliquant sur le bouton ci-dessous.</p>
<p>En cas de suppression, toutes vos données personnelles seront définitivement effacées du Registre de preuve de covoiturage. Vous ne serez pas supprimé des newsletters auxquelles vous avez pu vous inscrire.</p>
`;

const message_text =
  `Nous avons remarqué que vous n'avez pas utilisé votre compte Registre de preuve de covoiturage depuis plus d'un an. Pour rappel, celui-ci vous donne accès à votre espace partenaire pour effectuer des exports de données ou consulter une campagne d'incitations financières.

Conformément à notre politique de confidentialité et aux exigences du Règlement Général sur la Protection des Données (RGPD), votre compte sera automatiquement supprimé dans 7 jours si aucune activité n'est détectée d'ici là.

Si vous souhaitez conserver votre compte, reconnectez-vous via le lien ci-dessous.

En cas de suppression, toutes vos données personnelles seront définitivement effacées. Vous ne serez pas supprimé des newsletters auxquelles vous avez pu vous inscrire.`;

const defaultData: Partial<InactiveUserDeletionWarningTemplateData> = {
  title: "Votre compte va être supprimé",
  preview: "Votre compte Registre de preuve de covoiturage sera supprimé dans 7 jours.",
  action_message: "Je me connecte au RPC",
  message_html,
  message_text,
};

export class InactiveUserDeletionWarningNotification extends DefaultNotification {
  static override readonly subject = "Votre compte Registre de preuve de covoiturage va être supprimé";
  constructor(to: string, data: Partial<InactiveUserDeletionWarningTemplateData>) {
    super(to, { ...defaultData, ...data });
  }
}
