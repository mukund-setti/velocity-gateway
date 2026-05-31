-- Velo cache schema. The gateway will also run CREATE IF NOT EXISTS on
-- startup, but having it here means the table exists right after `docker
-- compose up postgres`.
CREATE EXTENSION IF NOT EXISTS vector;
