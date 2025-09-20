CREATE EXTERNAL TABLE `stedi`.`accelerometer_landing` ( `user` string, `timestamp` bigint, `x` float, `y` float, `z` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://your-project-stedi-lakehouse/accelerometer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false')
