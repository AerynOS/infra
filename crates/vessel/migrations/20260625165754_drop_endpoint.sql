-- Bye bye endpoint table
DROP TABLE IF EXISTS endpoint;

-- No such thing as a service account anymore. Service
-- auth is handled directly / account DB isn't referenced.
DELETE FROM account
WHERE type = 'service';
