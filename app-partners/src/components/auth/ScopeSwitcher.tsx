"use client";
import { type UserScope } from "@/interfaces/auth";
import { useAuth } from "@/providers/AuthProvider";
import { fr } from "@codegouvfr/react-dsfr";
import Badge from "@codegouvfr/react-dsfr/Badge";
import Autocomplete from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";

// Sélecteur du périmètre actif dans le header (bascule serveur réelle, pas le mode simulate).
export function ScopeSwitcher() {
  const { isAuth, scopes, activeScope, switchScope } = useAuth();
  if (!isAuth) return null;

  // Seuls les territoires sont basculables (opérateur = 1:1).
  const territoryScopes = scopes.filter((s) => s.territory_id);

  // 0 ou 1 périmètre basculable : badge statique, pas de menu.
  if (territoryScopes.length <= 1) {
    if (!activeScope) return null;
    return (
      <Badge as="span" severity="info" noIcon>
        {activeScope.label}
      </Badge>
    );
  }

  return (
    <Autocomplete
      id="scope-switcher"
      size="small"
      disableClearable
      options={territoryScopes}
      value={activeScope}
      isOptionEqualToValue={(o, v) => o.territory_id === v.territory_id}
      getOptionLabel={(option) => option.label}
      noOptionsText="Aucun périmètre"
      renderOption={(props, option) => {
        const isActive = option.territory_id === activeScope?.territory_id;
        return (
          <li {...props} key={option.territory_id}>
            <span className={fr.cx(isActive ? "fr-text--bold" : undefined)}>
              {isActive ? "✓ " : ""}
              {option.label}
            </span>
          </li>
        );
      }}
      onChange={(e, v: UserScope | null) => {
        if (v?.territory_id) void switchScope(v.territory_id);
      }}
      renderInput={(params) => <TextField {...params} label="Périmètre actif" />}
      sx={{ minWidth: 240 }}
    />
  );
}
