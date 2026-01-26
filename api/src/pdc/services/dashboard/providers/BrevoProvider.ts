import { ConfigInterfaceResolver, provider } from "@/ilos/common/index.ts";
import {
  BrevoProviderInterface,
  BrevoProviderInterfaceResolver,
  SendWelcomeEmailParams,
} from "../interfaces/BrevoProviderInterface.ts";

@provider({
  identifier: BrevoProviderInterfaceResolver,
})
export class BrevoProvider implements BrevoProviderInterface {
  private apiUrl: string = "";
  private apiKey: string = "";
  private welcomeTemplateId: number = 0;

  constructor(private config: ConfigInterfaceResolver) {}

  async init(): Promise<void> {
    this.apiUrl = this.config.get("brevo.apiUrl", "");
    this.apiKey = this.config.get("brevo.apiKey", "");
    this.welcomeTemplateId = this.config.get("brevo.welcomeTemplateId", 0);
  }

  async sendWelcomeEmail(params: SendWelcomeEmailParams): Promise<void> {
    if (!this.apiUrl || !this.apiKey || !this.welcomeTemplateId) {
      return;
    }

    const payload = {
      to: [{ email: params.email }],
      templateId: this.welcomeTemplateId,
      params: {
        email: params.email,
        siret: params.siret,
      },
    };

    const response = await fetch(this.apiUrl, {
      method: "POST",
      headers: {
        "accept": "application/json",
        "api-key": this.apiKey,
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const errorBody = await response.text();
      throw new Error(`Brevo API error: ${response.status} ${errorBody}`);
    }
  }
}
