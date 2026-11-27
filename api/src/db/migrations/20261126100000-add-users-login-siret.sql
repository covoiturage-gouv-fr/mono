ALTER TABLE auth.users ADD COLUMN IF NOT EXISTS login_siret varchar(14);

ALTER TABLE auth.users DROP CONSTRAINT IF EXISTS users_login_siret_format;
ALTER TABLE auth.users ADD CONSTRAINT users_login_siret_format CHECK (login_siret ~ '^[0-9]{14}$');

-- Reprise : SIRET de l'opérateur ou de la structure d'un territoire du compte (défaut en premier),
-- seulement s'il correspond au SIREN déjà saisi. Les autres comptes gardent login_siren en repli.
UPDATE auth.users u
SET login_siret = s.siret
FROM (
  SELECT DISTINCT ON (us.user_id) us.user_id, COALESCE(o.siret, c.siret) AS siret
  FROM auth.user_scopes us
  JOIN auth.users u2 ON u2._id = us.user_id
  LEFT JOIN operator.operators o ON o._id = us.operator_id
  LEFT JOIN territory.territory_group g ON g._id = us.territory_id
  LEFT JOIN company.companies c ON c._id = g.company_id
  WHERE COALESCE(o.siret, c.siret) ~ '^[0-9]{14}$'
    AND LEFT(COALESCE(o.siret, c.siret), 9) = u2.login_siren
  ORDER BY us.user_id, us.is_default DESC, us._id ASC
) s
WHERE u._id = s.user_id
  AND u.login_siret IS NULL
  AND u.login_siren IS NOT NULL;
