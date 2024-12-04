-- Drop the table if it exists
DROP TABLE IF EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}`;

-- Create a temporary table to combine data from two queries
CREATE TEMP TABLE table_ld_tmp AS
SELECT 
    {{ params.old_cols_origin }},
    FlightDate AS flight_dt,
    rundate,
    insert_dt
FROM 
    `{{ params.my_project }}.{{ params.my_serv_dataset }}.{{ params.serv_table_name }}`
UNION DISTINCT
SELECT 
    {{ params.old_cols_dest }},
    FlightDate AS flight_dt,
    rundate,
    insert_dt
FROM
    `{{ params.my_project }}.{{ params.my_serv_dataset }}.{{ params.serv_table_name }}`;

-- Create the final table with deduplicated and transformed data
CREATE TABLE `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}` AS
WITH table_ld AS (
    SELECT 
        {{ params.new_cols }},
        rundate,
        insert_dt,
        ROW_NUMBER() OVER (PARTITION BY {{ params.nk }} ORDER BY insert_dt DESC, flight_dt DESC) AS _rnk
    FROM 
        table_ld_tmp
    WHERE 
        insert_dt > TIMESTAMP('{{ task_instance.xcom_pull(task_ids="staging_layer.get_max_timestamp", key="max_timestamp") }}')
)
SELECT 
    {{ params.cast_typed_cols }},
    CURRENT_DATE() AS effective_start_dt,
    DATE('9999-12-31') AS effective_end_dt,
    1 AS active_flg,
    rundate,
    CURRENT_DATE() AS insert_date,
    CURRENT_DATE() AS update_date
FROM 
    table_ld
WHERE 
    _rnk = 1;
