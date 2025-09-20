CREATE TABLE customer_curated AS
SELECT DISTINCT ct.*
FROM customer_trusted ct
JOIN accelerometer_trusted at ON ct.email = at.user
