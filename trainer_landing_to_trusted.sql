CREATE TABLE step_trainer_trusted AS
SELECT DISTINCT 
    stl.sensorreadingtime,
    stl.serialnumber,
    stl.distancefromobject
FROM step_trainer_landing stl
JOIN customer_curated cc ON cc.serialnumber = stl.serialnumber
