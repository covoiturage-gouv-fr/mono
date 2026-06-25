"use client";

import { searchAddress } from "@/services/address.service";
import { Autocomplete, AutocompleteProps } from "./Autocomplete";

type AddressAutocompleteProps = Omit<AutocompleteProps, "search">;

/**
 * Address field bound to the Base Adresse Nationale (api-adresse.data.gouv.fr).
 * Thin wrapper around the generic <Autocomplete>: only injects the BAN search.
 */
export function AddressAutocomplete({ minChars = 3, ...props }: AddressAutocompleteProps) {
  return <Autocomplete {...props} minChars={minChars} search={searchAddress} />;
}
