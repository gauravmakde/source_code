GCS Bucket location: https://console.cloud.google.com/storage/browser/wellshadoopmigration-bucket?inv=1&invt=Ab0BLA&project=wf-mdss-bq&pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))

Use case 1
-- Creating the table on top of existing parque file in GCS bucket

CREATE OR REPLACE TABLE iceberg_dataset.usecase1
(
   id INT,
   name STRING,
   age STRING
)
with connection DEFAULT 
 options(
          file_format='parquet',
          table_format='iceberg',
          storage_uri='gs://wellshadoopmigration-bucket/use_Case1/'
          )  ;


--but created empty table 
-- I can do all the DML operations on this table

INSERT INTO iceberg_dataset.usecase1 (id, name, age)
VALUES
  (1, 'Alice', '25'),
  (2, 'Bob', '30'),
  (3, 'Charlie', '28'),
  (4, 'Diana', '35'),
  (5, 'Ethan', '40');

-- I have updated the name of the record with id=2 to 'Gaurav'
update iceberg_dataset.usecase1 
set name ='Gaurav'
where id=2;

MERGE INTO iceberg_dataset.usecase1 T
USING (
  SELECT 1 AS id, 'Alice' AS name, '26' AS age UNION ALL
  SELECT 6, 'Fiona', '29'
) S
ON T.id = S.id
WHEN MATCHED THEN
  UPDATE SET name = S.name, age = S.age
WHEN NOT MATCHED THEN
  INSERT (id, name, age) VALUES (S.id, S.name, S.age);


---------------------------------------------------------------------------

Use case 2:
-- Creating the iceberg table and do all DML statements on it.

CREATE OR REPLACE TABLE iceberg_dataset.usecase2
(
   id INT,
   name STRING,
   age STRING
)
with connection DEFAULT 
 options(
          file_format='parquet',
          table_format='iceberg',
          storage_uri='gs://wellshadoopmigration-bucket/use_Case2/'
          )  ;
select * from wf-mdss-bq.iceberg_dataset.usecase2;

-- I can do all the DML operations on this table
INSERT INTO iceberg_dataset.usecase2 (id, name, age)
VALUES
  (1, 'Alice', '25'),
  (2, 'Bob', '30'),
  (3, 'Charlie', '28'),
  (4, 'Diana', '35'),
  (5, 'Ethan', '40');

update iceberg_dataset.usecase2 
set name ='Gaurav'
where id=2;

select * from iceberg_dataset.usecase2;
MERGE INTO iceberg_dataset.usecase2 T
USING (
  SELECT 1 AS id, 'Alice' AS name, '26' AS age UNION ALL
  SELECT 6, 'Fiona', '29'
) S
ON T.id = S.id
WHEN MATCHED THEN
  UPDATE SET name = S.name, age = S.age
WHEN NOT MATCHED THEN
  INSERT (id, name, age) VALUES (S.id, S.name, S.age);


select * from iceberg_dataset.usecase2;
---------------------------------------------------------------------
use case 3:
-- Creating the external table from the parquet file in GCS bucket

CREATE or replace EXTERNAL TABLE `iceberg_dataset.external_table`
WITH CONNECTION default
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://wellshadoopmigration-bucket/sample_parque/sample_data.parquet']
);

CREATE OR REPLACE TABLE iceberg_dataset.usecase3
(
   id INT,
   name STRING,
   age STRING
)
with connection DEFAULT 
 options(
          file_format='parquet',
          table_format='iceberg',
          storage_uri='gs://wellshadoopmigration-bucket/use_Case3/'
          )  ;

-- I can insert the data from the external table to the iceberg table
INSERT INTO iceberg_dataset.usecase3 (id, name, age)
SELECT id, name, cast(age as string ) as age
FROM `wf-mdss-bq.iceberg_dataset.external_table`;

select * from   `iceberg_dataset.external_table`;
select * from iceberg_dataset.usecase3;
-------------------------------------

use case 4 

Creating native table and also while creating new bucket from iceberg table 

LOAD DATA OVERWRITE `wf-mdss-bq.iceberg_dataset.native_table`
FROM FILES (
    format = 'PARQUET',
    uris = ['gs://wellshadoopmigration-bucket/use_Case1/sample_data.parquet']
);

--------------------------
It failed bucket iceberg_table is not present
CREATE OR REPLACE TABLE iceberg_dataset.usecase4
(
   id INT,
   name STRING,
   age STRING
)
with connection DEFAULT 
 options(
          file_format='parquet',
          table_format='iceberg',
          storage_uri='gs://iceberg_table/use_Case4/'
          )  ;
-------------------------------
CREATE OR REPLACE TABLE iceberg_dataset.usecase4
(
   id INT,
   name STRING,
   age STRING
)
with connection DEFAULT 
 options(
          file_format='parquet',
          table_format='iceberg',
          storage_uri='gs://wellshadoopmigration-bucket/use_Case4/'
          )  ;

INSERT INTO iceberg_dataset.usecase4 (id, name, age)
SELECT id, name, cast(age as string ) as age
FROM `wf-mdss-bq.iceberg_dataset.native_table`;