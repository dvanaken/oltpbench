DROP TABLE IF EXISTS iotable;
CREATE TABLE iotable (
  empid int NOT NULL,
  data1 char(255) NOT NULL,
  data2 char(255) NOT NULL,
  data3 char(255) NOT NULL,
  data4 char(255) NOT NULL,
  data5 char(255) NOT NULL,
  data6 char(255) NOT NULL,
  data7 char(255) NOT NULL,
  data8 char(255) NOT NULL,
  data9 char(255) NOT NULL,
  data10 char(255) NOT NULL,
  data11 char(255) NOT NULL,
  data12 char(255) NOT NULL,
  data13 char(255) NOT NULL,
  data14 char(255) NOT NULL,
  data15 char(255) NOT NULL,
  data16 char(255) NOT NULL,
  PRIMARY KEY (empid)
);

DROP TABLE IF EXISTS iotablesmallrow;
CREATE TABLE iotablesmallrow (
  empid int NOT NULL,
  flag1 int NOT NULL,
  PRIMARY KEY (empid)
);

DROP TABLE IF EXISTS locktable;
CREATE TABLE locktable (
  empid int NOT NULL,
  salary int NOT NULL,
  PRIMARY KEY (empid)
);