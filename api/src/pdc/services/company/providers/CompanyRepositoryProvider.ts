import { provider } from "@/ilos/common/index.ts";
import { DenoPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql, { raw } from "@/lib/pg/sql.ts";

import {
  CompanyRepositoryProviderInterface,
  CompanyRepositoryProviderInterfaceResolver,
} from "../interfaces/CompanyRepositoryProviderInterface.ts";

import { CompanyInterface } from "@/shared/common/interfaces/CompanyInterface2.ts";

@provider({
  identifier: CompanyRepositoryProviderInterfaceResolver,
})
export class CompanyRepositoryProvider implements CompanyRepositoryProviderInterface {
  public readonly table = "company.companies";

  constructor(protected pgConnection: DenoPostgresConnection) {}

  async findById(id: number): Promise<CompanyInterface> {
    const rows = await this.pgConnection.query<CompanyInterface>(sql`
      SELECT
        _id,
        siret,
        siren,
        nic,
        legal_name,
        company_naf_code,
        establishment_naf_code,
        legal_nature_code,
        legal_nature_label,
        intra_vat,
        headquarter,
        updated_at,
        nonprofit_code,
        address,
        address_street,
        address_postcode,
        address_cedex,
        address_city,
        ST_X(geo::geometry) as lon,
        ST_Y(geo::geometry) as lat
      FROM ${raw(this.table)}
      WHERE _id = ${id}::int
      LIMIT 1
    `);

    return rows[0];
  }

  async findBySiret(siret: string): Promise<CompanyInterface> {
    const rows = await this.pgConnection.query<CompanyInterface>(sql`
      SELECT
        _id,
        siret,
        siren,
        nic,
        legal_name,
        company_naf_code,
        establishment_naf_code,
        legal_nature_code,
        legal_nature_label,
        intra_vat,
        headquarter,
        updated_at,
        nonprofit_code,
        address,
        address_street,
        address_postcode,
        address_cedex,
        address_city,
        ST_X(geo::geometry) as lon,
        ST_Y(geo::geometry) as lat
      FROM ${raw(this.table)}
      WHERE siret = ${siret}::varchar
      LIMIT 1
    `);

    return rows[0];
  }

  async updateOrCreate(data: CompanyInterface): Promise<void> {
    const now = new Date();
    const geo = data.lon ? `POINT(${data.lon} ${data.lat})` : null;

    const rows = await this.pgConnection.query<{ _id: number }>(sql`
      INSERT INTO ${raw(this.table)} (
        siret,
        siren,
        nic,
        legal_name,
        company_naf_code,
        establishment_naf_code,
        legal_nature_code,
        legal_nature_label,
        intra_vat,
        headquarter,
        updated_at,
        nonprofit_code,
        address,
        address_street,
        address_postcode,
        address_cedex,
        address_city,
        geo
      ) VALUES (
        ${data.siret},
        ${data.siren},
        ${data.nic},
        ${data.legal_name},
        ${data.company_naf_code},
        ${data.establishment_naf_code},
        ${data.legal_nature_code},
        ${data.legal_nature_label},
        ${data.intra_vat},
        ${data.headquarter},
        ${now},
        ${data.nonprofit_code},
        ${data.address},
        ${data.address_street},
        ${data.address_postcode},
        ${data.address_cedex},
        ${data.address_city},
        ${geo}
      )
      ON CONFLICT (siret)
      DO UPDATE SET
        siren = ${data.siren},
        nic = ${data.nic},
        legal_name = ${data.legal_name},
        company_naf_code = ${data.company_naf_code},
        establishment_naf_code = ${data.establishment_naf_code},
        legal_nature_code = ${data.legal_nature_code},
        legal_nature_label = ${data.legal_nature_label},
        intra_vat = ${data.intra_vat},
        headquarter = ${data.headquarter},
        updated_at = ${now},
        nonprofit_code = ${data.nonprofit_code},
        address = ${data.address},
        address_street = ${data.address_street},
        address_postcode = ${data.address_postcode},
        address_cedex = ${data.address_cedex},
        address_city = ${data.address_city},
        geo = ${geo}
      RETURNING _id
    `);

    if (rows.length !== 1) {
      throw new Error(`Unable to create or update company (${data.siret})`);
    }
  }
}
