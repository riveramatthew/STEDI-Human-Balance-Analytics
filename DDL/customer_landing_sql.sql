-- STEDI Customer Landing Table
-- This table contains raw customer data from the landing zone
-- Location: s3://stedi-s3/customer/landing/

CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-project`.`customer_landing` (
    `serialnumber` STRING,
    `sharewithpublicasofdate` BIGINT,
    `birthday` STRING,
    `registrationdate` BIGINT,
    `sharewithresearchasofdate` BIGINT,
    `customername` STRING,
    `email` STRING,
    `lastupdatedate` BIGINT,
    `phone` STRING,
    `sharewithfriendsasofdate` BIGINT
)
COMMENT 'Customer landing zone table with raw customer registration data'
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'FALSE',
    'dots.in.keys' = 'FALSE',
    'case.insensitive' = 'TRUE',
    'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-s3/customer/landing/'
TBLPROPERTIES (
    'classification' = 'json',
    'creator' = 'stedi-project',
    'last_modified' = 'auto'
);