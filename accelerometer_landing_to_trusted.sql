CREATE TABLE accelerometer_trusted AS
SELECT DISTINCT 
    al.user,
    al.timestamp,
    al.x,
    al.y,
    al.z
FROM accelerometer_landing al
JOIN customer_trusted ct ON ct.email = al.user
