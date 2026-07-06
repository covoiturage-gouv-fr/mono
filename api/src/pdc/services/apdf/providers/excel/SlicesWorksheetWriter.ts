import excel from "dep:excel";
import { provider } from "../../../../../ilos/common/index.ts";
import { SliceInterface } from "../../../policy/contracts/common/interfaces/Slices.ts";
import { SliceStatInterface } from "../../contracts/interfaces/PolicySliceStatInterface.ts";
import { AbstractWorksheetWriter } from "./AbstractWorksheetWriter.ts";

@provider()
export class SlicesWorksheetWriter extends AbstractWorksheetWriter {
  public readonly WORKSHEET_NAME = "Synthèse par tranche";
  public readonly COLUMN_HEADERS_NORMAL = [
    'Tranche "période normale"',
    "Incitations RPC",
    "Tous les trajets",
    "Trajets incités",
    "Contribution passagers",
  ];
  public readonly COLUMN_HEADERS_BOOSTER = [
    'Tranche "période booster"',
    "Incitations RPC",
    "Tous les trajets",
    "Trajets incités",
    "Contribution passagers",
  ];

  async call(
    wbWriter: excel.stream.xlsx.WorkbookWriter,
    slices: SliceStatInterface[],
    withDeclared = false,
  ): Promise<void> {
    // GEN-643 : les campagnes exposant le delta déclaré vs calculé utilisent un
    // layout dédié (bandeaux calculé/déclaré + bloc Delta). Le layout générique
    // ci-dessous reste strictement inchangé pour toutes les autres collectivités.
    if (withDeclared) {
      return this.callDeclared(wbWriter, slices);
    }

    const options: Partial<excel.AddWorksheetOptions> = {
      views: [{ showGridLines: false }],
    };
    const ws: excel.Worksheet = this.initWorkSheet(
      wbWriter,
      this.WORKSHEET_NAME,
      undefined,
      options,
    );

    // --------------------------------------------------------------------------------
    // Layout
    // --------------------------------------------------------------------------------
    const font = { name: "Arial", size: 12 };
    ws.getColumn("A").width = 26;
    ws.getColumn("A").font = font;
    ws.getColumn("B").width = 20;
    ws.getColumn("B").font = font;
    ws.getColumn("B").numFmt = "# ##0.00€";
    ws.getColumn("C").width = 20;
    ws.getColumn("C").font = font;
    ws.getColumn("D").width = 20;
    ws.getColumn("D").font = font;
    ws.getColumn("E").width = 20;
    ws.getColumn("E").font = font;
    ws.getColumn("E").numFmt = "# ##0.00€";

    // --------------------------------------------------------------------------------
    // Data
    // --------------------------------------------------------------------------------
    this.drawSliceTable(ws, slices, this.COLUMN_HEADERS_NORMAL, "normale");
    this.drawSliceTable(ws, slices, this.COLUMN_HEADERS_BOOSTER, "booster");

    // --------------------------------------------------------------------------------
    // Fields documentation
    // --------------------------------------------------------------------------------

    // Heading
    ws.mergeCells("A20:D20");
    ws.getCell("A20").value = "DEFINITIONS DES CHAMPS";
    ws.getCell("A20").font = { name: "Arial", size: 12, bold: true };
    ws.getCell("A20").alignment = { horizontal: "center", vertical: "middle" };
    ws.getCell("A20").border = { bottom: { style: "thin" } };

    ws.getCell("A22").value = "rpc_journey_id";
    ws.mergeCells("B22:D22");
    ws.getCell("B22").value = "Identifiant du trajet pour le RPC";

    ws.getCell("A23").value = "operator_journey_id";
    ws.mergeCells("B23:D23");
    ws.getCell("B23").value = "Identifiant du trajet envoyé par l'opérateur";

    ws.getCell("A24").value = "operator_trip_id";
    ws.mergeCells("B24:D24");
    ws.getCell("B24").value = "Identifiant de regroupement par conducteur envoyé par l'opérateur";

    ws.getCell("A25").value = "start_datetime";
    ws.mergeCells("B25:D25");
    ws.getCell("B25").value = "Date et heure de début du trajet";

    ws.getCell("A26").value = "end_datetime";
    ws.mergeCells("B26:D26");
    ws.getCell("B26").value = "Date et heure de fin du trajet";

    ws.getCell("A27").value = "start_location";
    ws.mergeCells("B27:D27");
    ws.getCell("B27").value = "Nom de la commune de départ";

    ws.getCell("A28").value = "start_epci";
    ws.mergeCells("B28:D28");
    ws.getCell("B28").value = "Nom de l'EPCI de départ";

    ws.getCell("A29").value = "start_insee";
    ws.mergeCells("B29:D29");
    ws.getCell("B29").value = "Code INSEE de la commune de départ";

    ws.getCell("A30").value = "end_location";
    ws.mergeCells("B30:D30");
    ws.getCell("B30").value = "Nom de la commune d'arrivée";

    ws.getCell("A31").value = "end_epci";
    ws.mergeCells("B31:D31");
    ws.getCell("B31").value = "Nom de l'EPCI d'arrivée";

    ws.getCell("A32").value = "end_insee";
    ws.mergeCells("B32:D32");
    ws.getCell("B32").value = "Code INSEE de la commune d'arrivée";

    ws.getCell("A33").value = "duration";
    ws.mergeCells("B33:D33");
    ws.getCell("B33").value = "Durée du trajet en secondes";

    ws.getCell("A34").value = "distance";
    ws.mergeCells("B34:D34");
    ws.getCell("B34").value = "Distance du trajet en mètres";

    ws.getCell("A35").value = "operator";
    ws.mergeCells("B35:D35");
    ws.getCell("B35").value = "Nom de l'opérateur";

    ws.getCell("A36").value = "operator_class";
    ws.mergeCells("B36:D36");
    ws.getCell("B36").value = "Classe du trajet envoyée par l'opérateur (A, B, C)";

    ws.getCell("A37").value = "driver_operator_user_id";
    ws.mergeCells("B37:D37");
    ws.getCell("B37").value = "Identifiant du conducteur envoyé par l'opérateur";

    ws.getCell("A38").value = "passenger_operator_user_id";
    ws.mergeCells("B38:D38");
    ws.getCell("B38").value = "Identifiant du passager envoyé par l'opérateur";

    ws.getCell("A39").value = "rpc_incentive";
    ws.mergeCells("B39:D39");
    ws.getCell("B39").value = "Montant d'incitation calculé par le RPC en euros";

    ws.getCell("A40").value = "incentive_type";
    ws.mergeCells("B40:D40");
    ws.getCell("B40").value = "Type d'incitation (normale ou booster)";

    ws.getCell("A41").value = "passenger_contribution";
    ws.mergeCells("B41:D41");
    ws.getCell("B41").value = "Contribution financière du passager en euros";

    // --------------------------------------------------------------------------------
    // Rules
    // --------------------------------------------------------------------------------

    // Heading
    ws.mergeCells("G1:L1");
    ws.getCell("G1").value = "RÈGLES DE CALCUL DES INCITATIONS RPC";
    ws.getCell("G1").font = { name: "Arial", size: 12, bold: true };
    ws.getCell("G1").alignment = { horizontal: "center", vertical: "middle" };
    ws.getCell("G1").border = { bottom: { style: "thin" } };

    // Rules body
    ws.mergeCells("G3:L5");
    ws.getCell("G3").value =
      "Ce fichier est un récapitulatif des incitations financières calculées par le RPC à la demande des collectivités souhaitant un suivi mensuel des incitations versées.";
    ws.getCell("G3").alignment = { wrapText: true, vertical: "top" };

    ws.mergeCells("G6:L10");
    ws.getCell("G7").value =
      'Le RPC applique alors le montant d\'incitation prévu aux trajets respectant les critères d\'acceptation RPC (status "ok" et "cancel") et les règles de la campagne d\'incitation résumées dans la "fiche descriptive de campagne" transmise par le RPC et validée par la collectivité et les opérateurs partenaires.';
    ws.getCell("G7").alignment = { wrapText: true, vertical: "top" };

    // Heading
    ws.mergeCells("G12:L12");
    ws.getCell("G12").value = "PRÉSENTATION DES ONGLETS";
    ws.getCell("G12").font = { name: "Arial", size: 12, bold: true };
    ws.getCell("G12").alignment = { horizontal: "center", vertical: "middle" };
    ws.getCell("G12").border = { bottom: { style: "thin" } };

    // First tab
    ws.mergeCells("G14:L14");
    ws.getCell("G14").value = {
      text: "Synthèse par tranche",
      hyperlink: "#'Synthèse par tranche'!A1",
    };
    ws.getCell("G14").font = { name: "Arial", size: 12, bold: true };
    ws.mergeCells("G15:L22");
    ws.getCell("G15").value =
      "La synthèse par tranche présente un résumé des montants d'incitations financières en fonction des différentes tranches tarifaires de la campagnes d'incitation.\n\nUn second tableau tranche \"période booster\" complète le premier pour les campagnes appliquant ponctuellement des règles de campagne différentes.";
    ws.getCell("G15").alignment = { wrapText: true, vertical: "top" };

    // Second tab
    ws.mergeCells("G23:L23");
    ws.getCell("G23").value = { text: "Trajets", hyperlink: "#'Trajets'!A1" };
    ws.getCell("G23").font = { name: "Arial", size: 12, bold: true };
    ws.mergeCells("G24:L28");
    ws.getCell("G24").value =
      "L'onglet \"Trajets\" présente l'ensemble des trajets éligibles réalisés par un opérateur donné sur un mois donné.\n\nVoir définition de chacun des champs dans notre documentation publique.";
    ws.getCell("G24").alignment = { wrapText: true, vertical: "top" };

    // Heading
    ws.mergeCells("G30:L30");
    ws.getCell("G30").value = "LIENS UTILES";
    ws.getCell("G30").font = { name: "Arial", size: 12, bold: true };
    ws.getCell("G30").alignment = { horizontal: "center", vertical: "middle" };
    ws.getCell("G30").border = { bottom: { style: "thin" } };

    // Link to documentation
    ws.mergeCells("G32:L32");
    ws.getCell(`G32`).value = {
      text: "Documentation générale",
      hyperlink: "https://doc.covoiturage.beta.gouv.fr",
    };
    ws.mergeCells("G33:L33");
    ws.getCell(`G33`).value = {
      text: "Documentation technique",
      hyperlink: "https://tech.covoiturage.beta.gouv.fr",
    };
    ws.mergeCells("G34:L34");
    ws.getCell(`G34`).value = {
      text: "Espace partenaire",
      hyperlink: "https://partenaire.covoiturage.beta.gouv.fr",
    };
    ws.mergeCells("G35:L35");
    ws.getCell(`G35`).value = {
      text: "Conditions Générale d'Utilisation du RPC",
      hyperlink: "https://doc.covoiturage.beta.gouv.fr/nos-services/api-primo-conducteurs-cee/cgu",
    };
    ws.mergeCells("G36:L36");
    ws.getCell(`G36`).value = {
      text: "contact@covoiturage.beta.gouv.fr",
      hyperlink: "mailto:contact@covoiturage.beta.gouv.fr",
    };

    ws.commit();
    /* eslint-enable prettier/prettier,max-len */
  }

  // ==================================================================================
  // GEN-643 — Layout dédié « delta déclaré vs calculé » (campagnes avec
  // extras.declared_incentives, ex. Covoit IDFM). Colonnes de l'onglet Trajets :
  // M distance · R rpc_incentive (calculé) · S incentive_type · T passenger_contribution
  // · U operator_declared_incentive (déclaré).
  // ==================================================================================

  private readonly BAND_CALCULATED = "Données calculées par covoiturage.beta";
  private readonly BAND_DECLARED = "Données déclarées par les opérateurs";
  private readonly DELTA_NOTE =
    "un écart de données peut exister du fait d'application de règles métier différentes entre l'opérateur et covoiturage.beta.gouv";
  private readonly CALC_FILL = "FFDDEBF7"; // bleu clair
  private readonly DECL_FILL = "FFFCE4D6"; // orange clair

  private async callDeclared(
    wbWriter: excel.stream.xlsx.WorkbookWriter,
    slices: SliceStatInterface[],
  ): Promise<void> {
    const options: Partial<excel.AddWorksheetOptions> = { views: [{ showGridLines: false }] };
    const ws: excel.Worksheet = this.initWorkSheet(wbWriter, this.WORKSHEET_NAME, undefined, options);

    // Column styling A..G (B/E/G en euros)
    const font = { name: "Arial", size: 12 };
    ws.getColumn("A").width = 26;
    ws.getColumn("A").font = font;
    for (const col of ["B", "C", "D", "E", "F", "G"]) {
      ws.getColumn(col).width = 20;
      ws.getColumn(col).font = font;
    }
    for (const col of ["B", "E", "G"]) {
      ws.getColumn(col).numFmt = "# ##0.00€";
    }

    // Le déclaré (E/F + bandeau) n'est exposé que sur la période normale. Le booster
    // ne porte que les colonnes calculées (B/C/D) + la contribution passagers (G).
    const normaleTotal = this.drawDeclaredSliceTable(ws, slices, 'Tranche "période normale"', "normale", true);
    this.drawDeclaredSliceTable(ws, slices, 'Tranche "période booster"', "booster", false);

    this.drawDeltaBlock(ws, normaleTotal);
    this.drawDeclaredDocumentation(ws);

    ws.commit();
  }

  /**
   * Dessine un tableau de tranches à 7 colonnes (A tranche ; B/C/D calculé ;
   * E/F/G déclaré) surmonté d'une ligne de bandeaux colorés. Renvoie le numéro
   * de la ligne de total (pour les références du bloc Delta).
   */
  private drawDeclaredSliceTable(
    ws: excel.Worksheet,
    slices: SliceStatInterface[],
    trancheLabel: string,
    mode: "normale" | "booster",
    withDeclaredColumns: boolean,
  ): number {
    if (ws.lastRow) this.pad(ws, 2);

    // Ligne de bandeaux (code couleur). Le bandeau « déclaré » (E:G) n'apparaît que
    // sur la période normale.
    const bandeau = ws.addRow([]);
    const bRow = bandeau.number;
    ws.mergeCells(`B${bRow}:D${bRow}`);
    ws.getCell(`B${bRow}`).value = this.BAND_CALCULATED;
    for (const col of ["B", "C", "D"]) this.fillCell(ws, `${col}${bRow}`, this.CALC_FILL);
    ws.getCell(`B${bRow}`).font = { name: "Arial", size: 11, bold: true };
    ws.getCell(`B${bRow}`).alignment = { horizontal: "center", vertical: "middle" };
    if (withDeclaredColumns) {
      ws.mergeCells(`E${bRow}:G${bRow}`);
      ws.getCell(`E${bRow}`).value = this.BAND_DECLARED;
      for (const col of ["E", "F", "G"]) this.fillCell(ws, `${col}${bRow}`, this.DECL_FILL);
      ws.getCell(`E${bRow}`).font = { name: "Arial", size: 11, bold: true };
      ws.getCell(`E${bRow}`).alignment = { horizontal: "center", vertical: "middle" };
    }

    // En-têtes (le booster masque les colonnes déclarées E/F)
    const headers = ws.addRow(
      withDeclaredColumns
        ? [trancheLabel, "Montant d'incitation", "Tous les trajets", "Trajets incités", "Montant d'incitation",
          "Trajets incités", "Contribution passagers"]
        : [trancheLabel, "Montant d'incitation", "Tous les trajets", "Trajets incités", null, null,
          "Contribution passagers"],
    );
    headers.font = { name: "Arial", size: 12, bold: true };
    headers.height = 24;
    headers.eachCell((c, colNumber) => {
      c.alignment = colNumber > 1 ? { vertical: "middle", horizontal: "right" } : { vertical: "middle" };
      c.border = { bottom: { style: "thin" } };
    });

    // Données
    for (const s of slices) {
      const { start, end } = s.slice;
      const mode_criteria = `Trajets!S:S,"${mode}"`;
      const slice_criteria = `Trajets!M:M,">=${start}"${end ? `,Trajets!M:M,"<${end}"` : ""}`;
      const calc = [
        this.formatSliceLabel(s.slice),
        { date1904: false, formula: `SUMIFS(Trajets!R:R,${mode_criteria},${slice_criteria})` },
        { date1904: false, formula: `COUNTIFS(${mode_criteria},${slice_criteria})` },
        { date1904: false, formula: `COUNTIFS(Trajets!R:R,">0",${mode_criteria},${slice_criteria})` },
      ];
      const contribution = { date1904: false, formula: `SUMIFS(Trajets!T:T,${mode_criteria},${slice_criteria})` };
      const r = ws.addRow(
        withDeclaredColumns
          ? [
            ...calc,
            { date1904: false, formula: `SUMIFS(Trajets!U:U,${mode_criteria},${slice_criteria})` },
            { date1904: false, formula: `COUNTIFS(Trajets!U:U,">0",${mode_criteria},${slice_criteria})` },
            contribution,
          ]
          : [...calc, null, null, contribution],
      );
      r.height = 20;
      r.alignment = { vertical: "middle" };
    }

    // Ligne de total (somme des colonnes présentes uniquement)
    const first = headers.number + 1;
    const last = headers.number + slices.length;
    const totalRow = last + 1;
    const border: Partial<excel.Borders> = { top: { style: "thin" } };
    const totalCols = withDeclaredColumns ? ["B", "C", "D", "E", "F", "G"] : ["B", "C", "D", "G"];
    for (const col of ["A", "B", "C", "D", "E", "F", "G"]) {
      const cell = ws.getCell(`${col}${totalRow}`);
      if (slices.length > 0 && totalCols.includes(col)) {
        cell.value = { date1904: false, formula: `SUM(${col}${first}:${col}${last})` };
      }
      cell.border = border;
    }
    return totalRow;
  }

  /**
   * Bloc « Delta » = calculé − déclaré (montant et volume de trajets incités).
   * Comparaison sur la seule période normale, seule à porter le déclaré.
   */
  private drawDeltaBlock(ws: excel.Worksheet, normaleTotal: number): void {
    this.pad(ws, 2);
    const label = ws.addRow(["Delta données covoiturage.beta / opérateurs"]);
    ws.mergeCells(`A${label.number}:G${label.number}`);
    ws.getCell(`A${label.number}`).font = { name: "Arial", size: 12, bold: true };
    ws.getCell(`A${label.number}`).border = { bottom: { style: "thin" } };

    const amount = ws.addRow([
      "Montant d'incitation",
      { date1904: false, formula: `B${normaleTotal}-E${normaleTotal}` },
    ]);
    ws.getCell(`B${amount.number}`).numFmt = "# ##0.00€";

    ws.addRow([
      "Trajets incités",
      { date1904: false, formula: `D${normaleTotal}-F${normaleTotal}` },
    ]);

    this.pad(ws, 1);
    const note = ws.addRow([this.DELTA_NOTE]);
    ws.mergeCells(`A${note.number}:G${note.number}`);
    ws.getCell(`A${note.number}`).font = { name: "Arial", size: 10, italic: true };
    ws.getCell(`A${note.number}`).alignment = { wrapText: true, vertical: "top" };
  }

  /** Définitions des champs (dont le déclaré + périmètre SIREN) placées sous les tableaux. */
  private drawDeclaredDocumentation(ws: excel.Worksheet): void {
    const defs: [string, string][] = [
      ["rpc_journey_id", "Identifiant du trajet pour le RPC"],
      ["operator_journey_id", "Identifiant du trajet envoyé par l'opérateur"],
      ["start_datetime", "Date et heure de début du trajet"],
      ["distance", "Distance du trajet en mètres"],
      ["operator", "Nom de l'opérateur"],
      ["rpc_incentive", "Montant d'incitation calculé par le RPC en euros"],
      ["incentive_type", "Type d'incitation (normale ou booster)"],
      ["passenger_contribution", "Contribution financière du passager en euros"],
      [
        "operator_declared_incentive",
        "Montant d'incitation déclaré par l'opérateur pour le territoire financeur de la campagne, " +
        "apparié par le SIREN (9 chiffres) de ce territoire, en euros. " +
        "Un trajet peut être financé par plusieurs entités : seul le montant déclaré pour ce territoire est repris.",
      ],
    ];

    this.pad(ws, 2);
    const head = ws.addRow(["DEFINITIONS DES CHAMPS"]);
    ws.mergeCells(`A${head.number}:G${head.number}`);
    ws.getCell(`A${head.number}`).font = { name: "Arial", size: 12, bold: true };
    ws.getCell(`A${head.number}`).alignment = { horizontal: "center", vertical: "middle" };
    ws.getCell(`A${head.number}`).border = { bottom: { style: "thin" } };

    for (const [key, description] of defs) {
      const r = ws.addRow([key]);
      ws.mergeCells(`B${r.number}:G${r.number}`);
      ws.getCell(`B${r.number}`).value = description;
      ws.getCell(`B${r.number}`).alignment = { wrapText: true, vertical: "top" };
    }
  }

  private fillCell(ws: excel.Worksheet, address: string, argb: string): void {
    ws.getCell(address).fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb },
    };
  }

  private drawSliceTable(
    ws: excel.Worksheet,
    slices: SliceStatInterface[],
    columns: string[],
    mode: "normale" | "booster",
  ): void {
    const offset = ws.lastRow ? ws.lastRow.number : 0;

    // apply some margin between tables with empty rows
    const margin = ws.lastRow ? 2 : 0;
    this.pad(ws, margin);

    // headers
    const headers = ws.addRow(columns);
    headers.font = { name: "Arial", size: 12, bold: true };
    headers.height = 24;
    headers.eachCell((c, colNumber) => {
      if (colNumber > 1) {
        c.alignment = { vertical: "middle", horizontal: "right" };
      } else c.alignment = { vertical: "middle" };
      c.border = { bottom: { style: "thin" } };
    });

    // data
    // S : incentive_type
    // M : distance
    // R : rpc_incentive
    // T : passenger_contribution
    for (const s of slices) {
      const { start, end } = s.slice;
      const mode_criteria = `Trajets!S:S,"${mode}"`;
      const slice_criteria = `Trajets!M:M,">=${start}"${end ? `,Trajets!M:M,"<${end}"` : ""}`;

      // const r = ws.addRow([this.formatSliceLabel(s.slice), s.sum / 100, s.count, s.subsidized]);
      const r = ws.addRow([
        this.formatSliceLabel(s.slice),
        {
          date1904: false,
          formula: `SUMIFS(Trajets!R:R,${mode_criteria},${slice_criteria})`,
        },
        {
          date1904: false,
          formula: `COUNTIFS(${mode_criteria},${slice_criteria})`,
        },
        {
          date1904: false,
          formula: `COUNTIFS(Trajets!R:R,">0",${mode_criteria},${slice_criteria})`,
        },
        {
          date1904: false,
          formula: `SUMIFS(Trajets!T:T,${mode_criteria},${slice_criteria})`,
        },
      ]);
      r.height = 20;
      r.alignment = { vertical: "middle" };
    }

    // add some totals at the bottom of the table
    const first = offset + margin + 2;
    const last = offset + margin + slices.length + 1;
    const border: Partial<excel.Borders> = { top: { style: "thin" } };
    ws.getCell(`A${last + 1}`).border = border;
    ws.getCell(`B${last + 1}`).value = {
      formula: `SUM(B${first}:B${last})`,
      date1904: false,
    };
    ws.getCell(`B${last + 1}`).border = border;
    ws.getCell(`C${last + 1}`).value = {
      formula: `SUM(C${first}:C${last})`,
      date1904: false,
    };
    ws.getCell(`C${last + 1}`).border = border;
    ws.getCell(`D${last + 1}`).value = {
      formula: `SUM(D${first}:D${last})`,
      date1904: false,
    };
    ws.getCell(`D${last + 1}`).border = border;
    ws.getCell(`E${last + 1}`).value = {
      formula: `SUM(E${first}:E${last})`,
      date1904: false,
    };
    ws.getCell(`E${last + 1}`).border = border;
  }

  private pad(ws: excel.Worksheet, n: number): void {
    for (let i = 0; i < n; i++) {
      ws.addRow([]);
    }
  }

  private formatSliceLabel(slice: SliceInterface): string {
    if (!slice.start && slice.end) {
      return `Jusqu'à ${slice.end / 1000} km`;
    } else if (slice.start && !slice.end) {
      return `Supérieure à ${slice.start / 1000} km`;
    }
    return `De ${slice.start / 1000} km à ${(slice.end || 0) / 1000} km`;
  }
}
