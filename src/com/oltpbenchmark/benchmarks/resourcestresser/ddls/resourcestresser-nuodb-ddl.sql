DROP TABLE IF EXISTS io1_table CASCADE;
CREATE TABLE io1_table (
  val int NOT NULL
);

DROP TABLE IF EXISTS io2_table1 CASCADE;
CREATE TABLE io2_table1 (
  val int NOT NULL
);

DROP TABLE IF EXISTS io2_table2 CASCADE;
CREATE TABLE io2_table2 (
  val int NOT NULL
);

DROP TABLE IF EXISTS locktable CASCADE;
CREATE TABLE locktable (
  empid int NOT NULL,
  salary int NOT NULL,
  PRIMARY KEY (empid)
);
