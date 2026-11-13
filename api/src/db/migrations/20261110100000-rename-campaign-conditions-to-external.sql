-- Campagnes déclarées : policy.campaign_conditions devient policy.external (séquence, index et contraintes suivent).
ALTER TABLE policy.campaign_conditions RENAME TO external;
ALTER SEQUENCE policy.campaign_conditions__id_seq RENAME TO external__id_seq;

DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT conname FROM pg_constraint
    WHERE conrelid = 'policy.external'::regclass AND conname LIKE 'campaign\_conditions\_%'
  LOOP
    EXECUTE format('ALTER TABLE policy.external RENAME CONSTRAINT %I TO %I', r.conname, replace(r.conname, 'campaign_conditions', 'external'));
  END LOOP;

  FOR r IN
    SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
    WHERE i.indrelid = 'policy.external'::regclass AND c.relname LIKE 'campaign\_conditions\_%'
  LOOP
    EXECUTE format('ALTER INDEX policy.%I RENAME TO %I', r.relname, replace(r.relname, 'campaign_conditions', 'external'));
  END LOOP;
END $$;
