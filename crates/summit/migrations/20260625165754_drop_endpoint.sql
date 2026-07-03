CREATE TABLE IF NOT EXISTS builder (
    public_key TEXT PRIMARY KEY,  
    description TEXT,
    size TEXT
);

INSERT INTO builder
(
  public_key,
  description
)
SELECT
  a.public_key,
  e.description
FROM
  endpoint e
  JOIN account a USING(account_id)
WHERE
  e.role = 'builder';

-- Update allocated builder to be the public key
-- of the builder. This is the new primary key.
WITH updated_builder AS (
  SELECT
    t.task_id,
    a.public_key
  FROM
    task t
    LEFT JOIN endpoint e ON t.allocated_builder = e.endpoint_id
    LEFT JOIN account a ON e.account_id = a.account_id
)
UPDATE
  task AS t
SET
  allocated_builder = u.public_key
FROM
  updated_builder u
WHERE
  t.task_id = u.task_id;

-- Bye bye endpoint table
DROP TABLE IF EXISTS endpoint;

-- No such thing as a service account anymore. Service
-- auth is handled directly / account DB isn't referenced.
DELETE FROM account
WHERE type = 'service';
