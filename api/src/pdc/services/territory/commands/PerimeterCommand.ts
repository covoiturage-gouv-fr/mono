import { command, CommandInterface, ResultType } from "@/ilos/common/index.ts";
import { castUserStringToUTC, toTzString } from "@/pdc/helpers/dates.helper.ts";
import { TerritoryPerimeterInterface } from "../contracts/common/interfaces/TerritoryPerimeterInterface.ts";
import {
  applyOperation,
  diffArr,
  findVersionAt,
  parseTerritoryCodes,
  successors,
  TerritoryOperation,
} from "../helpers/perimeters.ts";
import { PerimeterRepositoryProviderInterfaceResolver } from "../interfaces/PerimeterRepositoryProviderInterface.ts";

interface Options {
  territory?: number;
  all?: boolean;
  name?: string;
  siret?: string;
  from?: string;
  to?: string;
  at?: string;
  toVersion?: number;
  yes?: boolean;
  dryRun?: boolean;
}

interface Territory {
  _id: number;
  name: string;
}

const MAX_LISTED = 50;
// start of the RPC data, so that exports and statistics cover the whole history
const CREATE_FROM = "2019-01-01";

@command({
  signature: "territory:perimeter <action> [codes...]",
  description: "Périmètre d'un territoire : create | show | history | add | remove | set | rollback | remap. " +
    "Codes type:code (arr|com|epci|aom|dep|reg), ex. epci:200000172 com:74056",
  options: [
    {
      signature: "-t, --territory <territory>",
      description: "territory_id",
      coerce: (s: string) => parseInt(s, 10),
    },
    {
      signature: "--all",
      description: "remap : tous les territoires ayant des versions",
    },
    {
      signature: "-n, --name <name>",
      description: "create : nom du territoire",
    },
    {
      signature: "-s, --siret <siret>",
      description: "create : SIRET de l'entité (optionnel)",
    },
    {
      signature: "-f, --from <from>",
      description: `début de validité <YYYY-MM-DD> (défaut : maintenant, ${CREATE_FROM} pour create)`,
    },
    {
      signature: "--to <to>",
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
export class PerimeterCommand implements CommandInterface {
  constructor(protected repository: PerimeterRepositoryProviderInterfaceResolver) {}

  public async call(action: string, codes: string[], options: Options): Promise<ResultType> {
    switch (action) {
      case "create":
        return await this.create(codes, options);
      case "remap":
        if (options.all) {
          for (const territory_id of await this.repository.findTerritoriesWithVersions()) {
            await this.remap(await this.territory(territory_id), options);
          }
          return;
        }
        return await this.remap(await this.territory(options.territory), options);
      case "show":
        return await this.show(await this.territory(options.territory), options);
      case "history":
        return await this.history(await this.territory(options.territory));
      case "add":
      case "remove":
      case "set":
        return await this.change(await this.territory(options.territory), action, codes, options);
      case "rollback":
        return await this.rollback(await this.territory(options.territory), options);
      default:
        throw new Error(`Action inconnue '${action}' (create|show|history|add|remove|set|rollback|remap)`);
    }
  }

  protected async territory(territory_id?: number): Promise<Territory> {
    if (!territory_id) {
      throw new Error("--territory est obligatoire");
    }
    const territory = await this.repository.findTerritory(territory_id);
    if (!territory) {
      throw new Error(`Territoire ${territory_id} introuvable`);
    }
    return territory;
  }

  protected async create(codes: string[], options: Options): Promise<void> {
    if (!options.name) {
      throw new Error("--name est obligatoire");
    }
    const existing = await this.repository.findTerritoryByName(options.name);
    if (existing) {
      throw new Error(`Le territoire '${existing.name}' existe déjà (${existing._id}), utiliser -t ${existing._id}`);
    }
    const { valid_from, valid_to } = this.validity({ ...options, from: options.from ?? CREATE_FROM });
    const arr = await this.resolve(codes);

    console.log(`Nouveau territoire '${options.name}'${options.siret ? ` (SIRET ${options.siret})` : ""}`);
    if (!await this.confirm([], arr, valid_from, valid_to, options)) return;

    const territory_id = await this.repository.createTerritory(options.name, options.siret, {
      arr,
      valid_from,
      valid_to,
    });
    console.log(`Territoire ${territory_id} créé (version 1).`);
  }

  protected async show(territory: Territory, options: Options): Promise<void> {
    const at = castUserStringToUTC(options.at) ?? new Date();
    const version = findVersionAt(await this.repository.findByTerritory(territory._id), at);
    const arr = await this.repository.getArr(territory._id, at);
    console.log(
      version ? this.label(version) : `Aucune version au ${this.day(at)} : sélecteurs, ${arr.length} arr`,
    );
    console.log(arr.join(" "));
  }

  protected async history(territory: Territory): Promise<void> {
    const versions = await this.repository.findByTerritory(territory._id);
    if (!versions.length) {
      console.log(`Aucune version : périmètre issu des sélecteurs du territoire ${territory._id}`);
      return;
    }
    let previous: string[] = [];
    for (const version of versions) {
      const { added, removed } = diffArr(previous, version.arr);
      console.log(`${this.label(version)}  +${added.length} -${removed.length}`);
      previous = version.arr;
    }
  }

  protected async change(
    territory: Territory,
    op: TerritoryOperation,
    codes: string[],
    options: Options,
  ): Promise<void> {
    const { valid_from, valid_to } = this.validity(options);
    const resolved = await this.resolve(codes);
    const current = await this.repository.getArr(territory._id, valid_from);
    await this.write(territory, current, applyOperation(op, current, resolved), valid_from, valid_to, options);
  }

  protected async rollback(territory: Territory, options: Options): Promise<void> {
    const versions = await this.repository.findByTerritory(territory._id);
    const target = versions.find((v) => v.version === options.toVersion);
    if (!target) {
      throw new Error(`Version ${options.toVersion} introuvable`);
    }
    const { valid_from, valid_to } = this.validity(options);
    const current = await this.repository.getArr(territory._id, valid_from);
    await this.write(territory, current, target.arr, valid_from, valid_to, options);
  }

  /**
   * Adds the codes that replaced the current ones in a newer millesime,
   * from --from (default: now) since earlier trips keep their former code.
   * Territories without version follow the millesimes through their selectors.
   */
  protected async remap(territory: Territory, options: Options): Promise<void> {
    const valid_from = castUserStringToUTC(options.from) ?? new Date();
    const base = findVersionAt(await this.repository.findByTerritory(territory._id), valid_from);
    if (!base) {
      console.log(`Territoire ${territory._id} : aucune version au ${this.day(valid_from)}, rien à remapper`);
      return;
    }
    const added = successors(base.arr, await this.repository.findEvolutions());
    if (!added.length) {
      console.log(`Territoire ${territory._id} : aucun code à remapper`);
      return;
    }
    const valid_to = castUserStringToUTC(options.to) ?? base.valid_to;
    await this.write(territory, base.arr, applyOperation("add", base.arr, added), valid_from, valid_to, options);
  }

  protected async resolve(codes: string[]): Promise<string[]> {
    const resolved = await this.repository.resolve(parseTerritoryCodes(codes));
    if (resolved.unknown.length) {
      throw new Error(`Codes inconnus : ${resolved.unknown.join(" ")}`);
    }
    return resolved.arr;
  }

  protected async write(
    territory: Territory,
    current: string[],
    next: string[],
    valid_from: Date,
    valid_to: Date | null,
    options: Options,
  ): Promise<void> {
    console.log(`Territoire ${territory._id} '${territory.name}'`);
    if (!await this.confirm(current, next, valid_from, valid_to, options)) return;

    const version = await this.repository.create(territory._id, { arr: next, valid_from, valid_to });
    console.log(`Version ${version.version} créée.`);

    const policies = await this.repository.findPolicies(territory._id);
    if (policies.length) {
      console.log(`Campagnes de ce territoire : ${policies.map((p) => `${p._id} (${p.status})`).join(", ")}`);
      console.log(
        `Les incitations existantes ne sont pas recalculées : ` +
          `just api campaign:apply -c <id> --override -f <YYYY-MM-DD> -t <YYYY-MM-DD>`,
      );
    }
  }

  protected async confirm(
    current: string[],
    next: string[],
    valid_from: Date,
    valid_to: Date | null,
    options: Options,
  ): Promise<boolean> {
    if (!next.length) {
      throw new Error("Le périmètre résultant est vide");
    }
    const { added, removed } = diffArr(current, next);
    if (!added.length && !removed.length) {
      throw new Error("Aucun changement par rapport à la version en vigueur");
    }

    const range = `${this.day(valid_from)} → ${valid_to ? this.day(valid_to) : "∞"}`;
    console.log(`${range} : ${current.length} → ${next.length} arr`);
    await this.printDiff("+", added);
    await this.printDiff("-", removed);

    if (options.dryRun) {
      return false;
    }
    if (!options.yes) {
      if (!Deno.stdin.isTerminal()) {
        throw new Error("Hors TTY : --yes obligatoire");
      }
      if (!confirm("Créer la nouvelle version ?")) {
        console.log("Annulé");
        return false;
      }
    }
    return true;
  }

  protected async printDiff(sign: string, arr: string[]): Promise<void> {
    if (!arr.length) return;
    const rows = await this.repository.describe(arr.slice(0, MAX_LISTED));
    const pop = rows.reduce((sum, r) => sum + (r.pop ?? 0), 0);
    console.log(`${sign}${arr.length} arr${arr.length <= MAX_LISTED ? ` (pop. ${pop})` : ""}`);
    for (const row of rows) {
      console.log(`  ${sign} ${row.arr} ${row.label}`);
    }
    if (arr.length > MAX_LISTED) {
      console.log(`  … et ${arr.length - MAX_LISTED} autres`);
    }
  }

  protected validity(options: Options): { valid_from: Date; valid_to: Date | null } {
    const valid_from = castUserStringToUTC(options.from) ?? new Date();
    const valid_to = castUserStringToUTC(options.to) ?? null;
    if (valid_to && valid_to <= valid_from) {
      throw new Error("--to doit être postérieur à --from");
    }
    return { valid_from, valid_to };
  }

  protected label(version: TerritoryPerimeterInterface): string {
    const to = version.valid_to ? this.day(version.valid_to) : "∞";
    return `v${version.version} [${this.day(version.valid_from)} → ${to}) ${version.arr.length} arr`;
  }

  protected day(d: Date): string {
    return toTzString(d, undefined, "yyyy-MM-dd");
  }
}
