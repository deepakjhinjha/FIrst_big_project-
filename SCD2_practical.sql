use database snow_project ;

list @snow_project.pro_stages.my_s3_stage;

create or replace table bronze_table(id int , name varchar  );

insert into bronze_table values(1 , 'rahul' ) , (2 , 'deepak');

select * from bronze_table;



CREATE OR REPLACE STREAM bronze_stream
ON TABLE bronze_table
APPEND_ONLY = FALSE;

select * from bronze_stream;

MERGE INTO silver_table s
USING (
    -- Row to expire old current record
    SELECT id, name, 'EXPIRE' AS action
    FROM bronze_stream
    WHERE METADATA$ACTION = 'INSERT'
    
    UNION ALL
    
    -- Row to insert new version
    SELECT id, name, 'INSERT' AS action
    FROM bronze_stream
    WHERE METADATA$ACTION = 'INSERT'
) b
ON s.id = b.id
AND s.iscurrent = TRUE
AND b.action = 'EXPIRE'

WHEN MATCHED AND s.name <> b.name THEN
    UPDATE SET iscurrent = FALSE

WHEN NOT MATCHED AND b.action = 'INSERT' THEN
    INSERT (id, name, iscurrent)
    VALUES (b.id, b.name, TRUE);






update bronze_table 
set name = 'tushar'
where id = 2 ;

delete from silver_table 
where id = '2' and iscurrent = 'false';

select * from bronze_stream;

select * from silver_table;
select * from bronze_table;

