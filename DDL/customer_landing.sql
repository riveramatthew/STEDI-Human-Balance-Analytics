CREATE EXTERNAL TABLE `stedi`.`customer_landing` ( `serialnumber` string, `sharewithpublicasofdate` bigint, `birthday` string, `registrationdate` bigint, `sharewithresearchasofdate` bigint, `customername` string, `email` string, `lastupdatedate` bigint, `phone` string, `sharewithfriendsasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://your-project-stedi-lakehouse/customer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false')
