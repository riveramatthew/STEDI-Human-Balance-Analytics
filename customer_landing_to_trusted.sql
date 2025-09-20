CREATE TABLE customer_trusted AS
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL
