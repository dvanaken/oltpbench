-- Drop all tables

BEGIN EXECUTE IMMEDIATE 'DROP TABLE "io1_table"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "io2_table1"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "io2_table2"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "locktable"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;

-- Create table

CREATE TABLE io1_table (
  val integer NOT NULL
);

CREATE TABLE io2_table1 (
  val integer NOT NULL
);

CREATE TABLE io2_table2 (
  val integer NOT NULL
);

CREATE TABLE locktable (
  empid integer NOT NULL,
  salary integer NOT NULL,
  PRIMARY KEY (empid)
);

-- Procedures

create or replace
function md5raw (text in varchar2)
return varchar2 is
hash_value varchar2(20);
begin
   hash_value := dbms_obfuscation_toolkit.md5 (input_string => text);
   return hash_value;
end;

create or replace
function md5(text in varchar2)
return varchar2 is
hash_value varchar2(32);
begin
    select lower(rawtohex(md5raw(text)))
    into hash_value
    from dual;
    return hash_value;
end;
