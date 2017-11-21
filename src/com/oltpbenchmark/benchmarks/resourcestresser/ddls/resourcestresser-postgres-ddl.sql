CREATE OR REPLACE FUNCTION table_exists(_tbl text) RETURNS boolean AS $$ BEGIN RETURN (SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = 'public' and tableName = _tbl)); END; $$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION make_lo(bytea) RETURNS oid AS $$ DECLARE loid oid; fd integer; bytes integer; BEGIN loid := lo_creat(-1); fd := lo_open(loid, 131072); bytes := lowrite(fd, $1); IF (bytes != LENGTH($1)) THEN RAISE EXCEPTION 'Not all data copied to blob'; END IF; PERFORM lo_close(fd); RETURN loid; END; $$ LANGUAGE plpgsql STRICT;
CREATE OR REPLACE FUNCTION gen_binary_string(sizekb int) RETURNS bytea AS $$ DECLARE str text := ''; arr text[]; BEGIN FOR i IN 1..32 LOOP str := str || md5(random()::text); END LOOP; arr := (select array_fill(str, ARRAY[sizekb])); RETURN decode(array_to_string(arr, ''), 'escape'); END; $$ LANGUAGE plpgsql;

DROP TABLE IF EXISTS iointexponential;
CREATE TABLE iointexponential (
  val int NOT NULL
);

DROP TABLE IF EXISTS ioint;
CREATE TABLE ioint (
  val int NOT NULL
);

DROP TABLE IF EXISTS iointstore;
CREATE TABLE iointstore (
  val int NOT NULL
);

DROP TABLE IF EXISTS iobinary;
CREATE TABLE iobinary (
  val bytea NOT NULL
);

DROP TABLE IF EXISTS iobinarystore;
CREATE TABLE iobinarystore (
  val bytea NOT NULL
);

DROP TABLE IF EXISTS ioblob;
CREATE TABLE ioblob (
  val oid NOT NULL
);


