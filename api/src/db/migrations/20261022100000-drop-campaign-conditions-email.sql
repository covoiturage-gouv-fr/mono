-- RGPD : l'email de contact pouvait être nominatif ; les collectivités se joignent via leurs comptes.
ALTER TABLE policy.campaign_conditions DROP COLUMN email_contact;
