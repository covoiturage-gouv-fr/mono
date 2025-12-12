import { AbstractTemplate } from "@/pdc/providers/template/index.ts";

import { AbstractMailNotification } from "../AbstractNotification.ts";

export interface DefaultTemplateData {
  app_url: string;
  contact_email: string;
  header_alt?: string;
  header_image_src?: string;
  hero_alt?: string;
  hero_image_src?: string;
  action_href?: string;
  action_message?: string;
  title: string;
  preview?: string;
  message_html?: string;
  message_text: string;
  fullname: string;
}

export class DefaultMJMLTemplate extends AbstractTemplate<DefaultTemplateData> {
  static override readonly template = `
<mjml version="4.6.3" lang="fr">
  <mj-head>
    <mj-title>
      {{ title }}
    </mj-title>
    {{#if preview}}
    <mj-preview>
      {{ preview }}
    </mj-preview>
    {{/if}}
    <mj-attributes>
      <mj-text color="#5e6977" font-family="Arial, sans-serif" font-size="13px" />
      <mj-section background-color="#ffffff" padding="0 0 0 0" text-align="center" />
      <mj-column padding="0 0 0 0" />
      <mj-image border="none" target="_blank" title="" height="auto" />
    </mj-attributes>
  </mj-head>
  <mj-body background-color="#f2f5f9">
    {{#if header_image_src}}
    <mj-section>
      <mj-column vertical-align="top">
        <mj-image
          align="left"
          alt="{{ header_alt }}"
          container-background-color="#ffffff"
          href="{{ app_url }}"
          padding="20px 20px 20px 20px"
          src="{{ header_image_src }}"
          width="1200px"
        ></mj-image>
      </mj-column>
    </mj-section>
    {{/if}}
    <mj-section>
    <mj-column>
        {{#if hero_image_src}}
        <mj-image
          align="center"
          alt="{{ hero_alt }}"
          href=""
          padding="10px 25px 10px 25px"
          src="{{ hero_image_src }}"
          width="304px"
        ></mj-image>
        {{/if}}
        <mj-text
          align="center"
          line-height="22px"
          padding="20px 0 20px 0"
          font-weight="700"
          font-size="23px"
        >
          <h1 style="font-size:23px;"> {{ title }} </h1>
        </mj-text>
      </mj-column>
    </mj-section>
    <mj-section>
      <mj-column>
        <mj-text align="left" line-height="22px" color="#000000" padding="20px 40px 40px 40px">
          <p>Bonjour {{fullname}},</p>
          {{#if message_html}}
          {{{ message_html }}}
          {{else}}
          <p>{{ message_text }}</p>
          {{/if}}
        </mj-text>
        {{#if action_href}}
        <mj-button
          href="{{action_href}}"
          rel="nofollow"
          font-family="Arial, sans-serif"
          background-color="#414141"
          color="#ffffff"
        >
          {{ action_message }}
        </mj-button>
        <mj-text align="left" line-height="22px" color="#000000" padding="20px 40px 40px 40px">
          <p>ou cliquez sur le lien suivant :</p>
          <p>{{action_href}}</p>
        </mj-text>
        {{/if}}
        <mj-text align="left" line-height="22px" color="#000000" padding="20px 40px 40px 40px">
          A bientôt
        </mj-text>
      </mj-column>
    </mj-section>
    <mj-section background-color="#f2f5f9">
      <mj-column vertical-align="middle">
        <mj-text
          align="center"
          line-height="20px"
          padding="10px 25px 0px 25px"
          font-weight="700"
          font-size="12px"
          color="#000000"
        >
          <span>Registre de preuve de covoiturage - {{ app_url }}</span>
        </mj-text>
        <mj-text
          align="center"
          line-height="20px"
          padding="10px 25px 0px 25px"
          font-size="12px"
          color="#000000"
        >
          <span>
            Un service fourni grâce au soutien financier de l'ADEME,
            puis de la DGITM et à l’appui stratégique et opérationnel de la DINUM.
          </span>
        </mj-text>
          <mj-text
          align="center"
          line-height="20px"
          padding="10px 25px 0px 25px"
          font-size="12px"
          color="#000000"
          >
            <span>
              ✉ <a style="color:#000000" href="mailto:{{contact_email}}">{{contact_email}}</a>
            </span>
        </mj-text>
        <mj-text
          align="center"
          line-height="20px"
          padding="10px 25px 0px 25px"
          font-size="12px"
          color="#000000"
        >
          <span>📍 
            <a
              target="_blank"
              style="color:#000000; font-size:12px"
              href="https://www.openstreetmap.org/node/2353712460"
            >
              20 avenue de Ségur, 75007 Paris
            </a>
          </span>
        </mj-text>
      </mj-column>
    </mj-section>
  </mj-body>
</mjml>
  `;
}

export class DefaultTextTemplate extends AbstractTemplate<DefaultTemplateData> {
  static override readonly template = `
Bonjour {{fullname}},

{{ message_text }}

{{#if action_href}}
{{ action_message }} [{{action_href}}]
{{/if}}

A bientôt
-- 
Registre de preuve de covoiturage - {{ app_url }}
Un service fourni grâce au soutien financier de l'ADEME,
puis de la DGITM et à l’appui stratégique et opérationnel de la DINUM.
✉ {{contact_email}}
📍 20 avenue de Ségur, 75007 Paris
  `;
}

export class DefaultNotification extends AbstractMailNotification<DefaultTemplateData> {
  static override templateText = DefaultTextTemplate;
  static override templateMJML = DefaultMJMLTemplate;
  static override readonly subject: string = "Registre de preuve de covoiturage";

  constructor(to: string, data: Partial<DefaultTemplateData>) {
    super(to, {
      app_url: "https://covoiturage.beta.gouv.fr",
      contact_email: "contact@covoiturage.beta.gouv.fr",
      header_image_src: "https://x0zwu.mjt.lu/tplimg/x0zwu/b/xp6yw/vkw1r.png",
      header_alt: "Registre de preuve de covoiturage",
      ...data,
    });
  }
}
