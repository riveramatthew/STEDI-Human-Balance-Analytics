-- STEDI Accelerometer Landing Table
-- This table contains raw accelerometer sensor data from the landing zone
-- Location: s3://stedi-s3/accelerometer/landing/

CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-project`.`accelerometer_landing` (
    `user` STRING,
    `timeStamp` BIGINT,
    `x` FLOAT,
    `y` FLOAT,
    `z` FLOAT
)
COMMENT 'Accelerometer landing zone table with raw sensor readings (x, y, z coordinates)'
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'FALSE',
    'dots.in.keys' = 'FALSE',
    'case.insensitive' = 'TRUE',
    'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-s3/accelerometer/landing/'
TBLPROPERTIES (
    'classification' = 'json',
    'creator' = 'stedi-project',
    'last_modified' = 'auto'
);