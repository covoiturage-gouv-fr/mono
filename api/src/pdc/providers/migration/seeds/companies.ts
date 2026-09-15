export interface Company {
  _id: number;
  siret: string;
  siren: string;
  nic: string;
  legal_name: string;
  company_naf_code: string;
  establishment_naf_code: string;
  headquarter: boolean;
  legal_nature_code?: string;
  legal_nature_label?: string;
  nonprofit_code?: string;
  intra_vat?: string;
  address?: string;
  updated_at?: Date;
  address_street?: string;
  address_postcode?: string;
  address_cedex?: string;
  address_city?: string;
}

function makeCompany(
  _id: number,
  siret: string,
  legal_name: string,
  company: Partial<Company> = {},
): Company {
  const [siren, nic] = [siret.slice(0, 9), siret.slice(9)];
  return {
    _id,
    siret,
    siren,
    nic,
    legal_name,
    company_naf_code: "6201Z",
    establishment_naf_code: "6201Z",
    headquarter: true,
    ...company,
  };
}

export const dinum = makeCompany(1, "13002526500013", "dinum");
export const metropoleLyon = makeCompany(2, "20004697700019", "metropole de lyon");

export const companies: Company[] = [dinum, metropoleLyon];
