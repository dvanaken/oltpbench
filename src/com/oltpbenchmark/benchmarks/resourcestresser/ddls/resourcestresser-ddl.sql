DROP TABLE IF EXISTS io1_table;
CREATE TABLE io1_table (
  val int NOT NULL
);

DROP TABLE IF EXISTS io2_table1;
CREATE TABLE io2_table1 (
  val int NOT NULL
);

DROP TABLE IF EXISTS io2_table2;
CREATE TABLE io2_table2 (
  val int NOT NULL
);

DROP TABLE IF EXISTS locktable;
CREATE TABLE locktable (
  empid int NOT NULL,
  salary int NOT NULL,
  PRIMARY KEY (empid)
);