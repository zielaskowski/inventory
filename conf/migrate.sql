-- migrate data from old to new.sqlite
-- tables with exact columns in old must exists in new
-- USAGE
-- sqlite3 new.sqlite < migrate.sql

-- attach the old database
ATTACH 'inventory_old.sqlite' AS old;

-- turn off foreign keys for the copy
PRAGMA foreign_keys = OFF;

-- BEGIN transaction for speed
BEGIN;

-- For every table in the old DB, copy data into the same table name in the main DB
-- This creates a set of INSERT commands dynamically
.output migrate_inserts.sql
SELECT 'INSERT INTO main.' || name || ' SELECT * FROM old.' || name || ';'
FROM old.sqlite_master
WHERE type='table' 
	AND name NOT LIKE 'sqlite_%'
	AND name NOT LIKE 'audite%';

.output stdout

-- Execute generated INSERTs
.read migrate_inserts.sql

COMMIT;
