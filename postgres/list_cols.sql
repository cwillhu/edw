
create schema if not exists edw;

create or replace function edw.list_cols (myschema text, mytable text) returns text as $$
    SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = myschema
        AND table_name = mytable
$$ language sql;
