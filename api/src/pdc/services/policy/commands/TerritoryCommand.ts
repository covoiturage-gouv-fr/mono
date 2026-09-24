import { command, CommandInterface, NotFoundException, ResultType } from "@/ilos/common/index.ts";
import { castUserStringToUTC, toTzString } from "../helpers/index.ts";
import {
  applyOperation,
  diffArr,
  findVersionAt,
  parseTerritoryCodes,
  TerritoryOperation,
} from "../helpers/territories.ts";
import {
  PolicyRepositoryProviderInterfaceResolver,
  PolicyTerritoryInterface,
  PolicyTerritoryRepositoryProviderInterfaceResolver,
  SerializedPolicyInterface,
} from "../interfaces/index.ts";

interface Options {
  campaign: number;
  from?: string;
  to?: string;
  at?: string;
  toVersion?: number;
  yes?: boolean;
  dryRun?: boolean;
}

const MAX_LISTED = 50;

@command({
  signature: "campaign:territory <action> [codes...]",
  description: "Périmètre d'une campagne : show | history | add | remove | set | rollback. Codes type:code " +
    "(arr|com|epci|aom|dep|reg|reseau|country), ex. aom:241700434 com:17300",
  options: [
    {
      signature: "-c, --campaign <campaign>",
      description: "campaign_id",
      coerce: (s: string) => parseInt(s, 10),
    },
    {
      signature: "-f, --from <from>",
      description: "début de validité <YYYY-MM-DD> (défaut : début de campagne)",
    },
    {
      signature: "-t, --to <to>",
      description: "fin de validité exclue <YYYY-MM-DD> (défaut : sans fin)",
    },
    {
      signature: "--at <at>",
      description: "show : date <YYYY-MM-DD> (défaut : maintenant)",
    },
    {
      signature: "--to-version <version>",
      description: "rollback : version à recopier",
      coerce: (s: string) => parseInt(s, 10),
    },
    {
      signature: "-y, --yes",
      description: "pas de confirmation (obligatoire hors TTY)",
    },
    {
      signature: "--dry-run",
      description: "affiche le diff sans écrire",
    },
  ],
})
export class TerritoryCommand implements CommandInterface {
  constructor(
    protected policyRepository: PolicyRepositoryProviderInterfaceResolver,
    protected territoryRepository: PolicyTerritoryRepositoryProviderInterfaceResolver,
  ) {}

  public async call(action: string, codes: string[], options: Options): Promise<ResultType> {
    if (!options.campaign) {
      throw new Error("--campaign est obligatoire");
    }

    const policy = await this.policyRepository.find(options.campaign);
    if (!policy) {
      throw new NotFoundException(`Campagne ${options.campaign} introuvable`);
    }
    const versions = await this.territoryRepository.findByPolicy(policy._id);

    switch (action) {
      case "show":
        return this.show(policy, versions, options);
      case "history":
        return this.history(policy, versions);
      case "add":
      case "remove":
      case "set":
        return await this.change(policy, versions, action, codes, options);
      case "rollback":
        return await this.rollback(policy, versions, options);
      default:
        throw new Error(`Action inconnue '${action}' (show|history|add|remove|set|rollback)`);
    }
  }

  protected show(policy: SerializedPolicyInterface, versions: PolicyTerritoryInterface[], options: Options): void {
    const at = castUserStringToUTC(options.at, policy.tz) ?? new Date();
    const version = findVersionAt(versions, at);
    if (!version) {
      console.log(`Aucune version au ${this.day(at, policy)} : repli sur territory_id ${policy.territory_id}`);
      return;
    }
    console.log(this.label(version, policy));
    console.log(version.arr.join(" "));
  }

  protected history(policy: SerializedPolicyInterface, versions: PolicyTerritoryInterface[]): void {
    if (!versions.length) {
      console.log(`Aucune version : périmètre issu de territory_id ${policy.territory_id}`);
      return;
    }
    let previous: string[] = [];
    for (const version of versions) {
      const { added, removed } = diffArr(previous, version.arr);
      console.log(`${this.label(version, policy)}  +${added.length} -${removed.length}`);
      previous = version.arr;
    }
  }

  protected async change(
    policy: SerializedPolicyInterface,
    versions: PolicyTerritoryInterface[],
    op: TerritoryOperation,
    codes: string[],
    options: Options,
  ): Promise<void> {
    const { valid_from, valid_to } = this.validity(policy, options);
    const resolved = await this.territoryRepository.resolve(parseTerritoryCodes(codes), valid_from.getUTCFullYear());
    if (resolved.unknown.length) {
      throw new Error(`Codes inconnus : ${resolved.unknown.map((c) => `${c.type}:${c.code}`).join(" ")}`);
    }

    const current = findVersionAt(versions, valid_from)?.arr ?? [];
    await this.write(policy, current, applyOperation(op, current, resolved.arr), valid_from, valid_to, options);
  }

  protected async rollback(
    policy: SerializedPolicyInterface,
    versions: PolicyTerritoryInterface[],
    options: Options,
  ): Promise<void> {
    const target = versions.find((v) => v.version === options.toVersion);
    if (!target) {
      throw new Error(`Version ${options.toVersion} introuvable`);
    }
    const { valid_from, valid_to } = this.validity(policy, options);
    const current = findVersionAt(versions, valid_from)?.arr ?? [];
    await this.write(policy, current, target.arr, valid_from, valid_to, options);
  }

  protected async write(
    policy: SerializedPolicyInterface,
    current: string[],
    next: string[],
    valid_from: Date,
    valid_to: Date | null,
    options: Options,
  ): Promise<void> {
    if (!next.length) {
      throw new Error("Le périmètre résultant est vide");
    }
    const { added, removed } = diffArr(current, next);
    if (!added.length && !removed.length) {
      throw new Error("Aucun changement par rapport à la version en vigueur");
    }

    const range = `${this.day(valid_from, policy)} → ${valid_to ? this.day(valid_to, policy) : "∞"}`;
    console.log(`Campagne ${policy._id} — ${range} : ${current.length} → ${next.length} arr`);
    await this.printDiff("+", added);
    await this.printDiff("-", removed);

    if (options.dryRun) {
      return;
    }
    if (!options.yes) {
      if (!Deno.stdin.isTerminal()) {
        throw new Error("Hors TTY : --yes obligatoire");
      }
      if (!confirm("Créer la nouvelle version ?")) {
        console.log("Annulé");
        return;
      }
    }

    const version = await this.territoryRepository.create(policy._id, { arr: next, valid_from, valid_to });
    console.log(`Version ${version.version} créée.`);
    console.log(
      `Les incitations existantes ne sont pas recalculées : ` +
        `just api campaign:apply -c ${policy._id} --override -f <YYYY-MM-DD> -t <YYYY-MM-DD>`,
    );
  }

  protected async printDiff(sign: string, arr: string[]): Promise<void> {
    if (!arr.length) return;
    const rows = await this.territoryRepository.describe(arr.slice(0, MAX_LISTED));
    const pop = rows.reduce((sum, r) => sum + (r.pop ?? 0), 0);
    console.log(`${sign}${arr.length} arr${arr.length <= MAX_LISTED ? ` (pop. ${pop})` : ""}`);
    for (const row of rows) {
      console.log(`  ${sign} ${row.arr} ${row.label}`);
    }
    if (arr.length > MAX_LISTED) {
      console.log(`  … et ${arr.length - MAX_LISTED} autres`);
    }
  }

  protected validity(policy: SerializedPolicyInterface, options: Options): { valid_from: Date; valid_to: Date | null } {
    const valid_from = castUserStringToUTC(options.from, policy.tz) ?? policy.start_date;
    const valid_to = castUserStringToUTC(options.to, policy.tz) ?? null;
    if (valid_to && valid_to <= valid_from) {
      throw new Error("--to doit être postérieur à --from");
    }
    return { valid_from, valid_to };
  }

  protected label(version: PolicyTerritoryInterface, policy: SerializedPolicyInterface): string {
    const to = version.valid_to ? this.day(version.valid_to, policy) : "∞";
    return `v${version.version} [${this.day(version.valid_from, policy)} → ${to}) ${version.arr.length} arr`;
  }

  protected day(d: Date, policy: SerializedPolicyInterface): string {
    return toTzString(d, policy.tz, "yyyy-MM-dd");
  }
}
