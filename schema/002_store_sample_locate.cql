-- Upgrade for a keyspace created before learning from locates.
-- A fresh install gets the column from 001_init.cql; this only adds it.
-- Running it twice fails harmlessly: the column already exists.
ALTER TABLE ichnos.api_key ADD store_sample_locate int;
