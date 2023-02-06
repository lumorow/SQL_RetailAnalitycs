CREATE role visitor with LOGIN PASSWORD '1111'
NOSUPERUSER NOCREATEDB NOREPLICATION INHERIT NOCREATEROLE;
GRANT CONNECT ON DATABASE sql_3 TO visitor;
GRANT USAGE ON SCHEMA public TO visitor;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO visitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES to visitor;

CREATE role admin
    SUPERUSER
    CREATEDB
    CREATEROLE
login
PASSWORD '1212';

---drop role reader
REASSIGN OWNED BY visitor TO postgres;
DROP OWNED BY visitor;
drop role visitor;

---drop role admin
REASSIGN OWNED BY admin TO postgres;
DROP OWNED BY admin;
drop role admin;


-- to change user
-- psql -U admin -W postgres