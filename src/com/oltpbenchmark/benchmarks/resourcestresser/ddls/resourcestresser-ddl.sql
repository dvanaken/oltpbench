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
  val varbinary(30) NOT NULL
);

DROP TABLE IF EXISTS iobinarystore;
CREATE TABLE iobinarystore (
  val longvarbinary(1G) NOT NULL
);

DROP TABLE IF EXISTS ioblob;
CREATE TABLE ioblob (
  val longvarbinary(1G) NOT NULL
);
