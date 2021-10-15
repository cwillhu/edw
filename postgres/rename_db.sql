
----
-- Rename database 'census' to 'edw'
----

\c postgres;

-- prevent new connections
revoke connect on database census from public;

-- display existing connections
SELECT pid, state, datname, usename, application_name, backend_type, query
FROM pg_stat_activity 
WHERE datname = 'census' AND pid <> pg_backend_pid();

-- close existing connections (may take some time)
SELECT pid, pg_terminate_backend(pid)
FROM pg_stat_activity 
WHERE datname = 'census' AND pid <> pg_backend_pid();

-- rename database
alter database census rename to edw;
