-- Drop Exisiting Tables

IF OBJECT_ID('[io1_table]') IS NOT NULL DROP table [dbo].[io1_table];
IF OBJECT_ID('[io2_table1]') IS NOT NULL DROP table [dbo].[io2_table1];
IF OBJECT_ID('[io2_table2]') IS NOT NULL DROP table [dbo].[io2_table2];

-- Create Tables

CREATE TABLE io1_table (
  val int NOT NULL
);

CREATE TABLE io2_table1 (
  val int NOT NULL
);

CREATE TABLE io2_table2 (
  val int NOT NULL
);