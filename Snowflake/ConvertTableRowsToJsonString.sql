select rows of tsble in json string format with chunk of 2000 records.

--1)
  create sample table to store records.
	create or replace TABLE IHS_MOBILITY.FCM_OWNER.TEMP_JSONDATA (
		RAW_DATA VARIANT
	);

-- select * from  temp_jsondata;
-- truncate table temp_jsondata

--2 types(static table and dynamic table) of script to convert data in json.

  --1) --STATIC TABLE 
	insert into temp_jsonData (raw_data) 
	SELECT parse_json('[' || json_data || ']') AS json_data_array
	FROM (SELECT
		 ARRAY_TO_STRING(
			ARRAY_AGG(
				TO_JSON(
					OBJECT_CONSTRUCT_KEEP_NULL(
				'VEHICLE_ID', VEHICLE_ID,
				'SALES_ISO2_COUNTRY_CODE',SALES_ISO2_COUNTRY_CODE,
				'VEHICLE_SALES_REGIONAL_ORIGIN',VEHICLE_SALES_REGIONAL_ORIGIN,
				'VEHICLE_SALES_LAST_ACTUAL_DATE',VEHICLE_SALES_LAST_ACTUAL_DATE,
				'VEHICLE_SALES_FORECAST_PUBLISHED_DATE',VEHICLE_SALES_FORECAST_PUBLISHED_DATE,
				'VEHICLE_SALES_MONTHS_IN_MARKET', VEHICLE_SALES_MONTHS_IN_MARKET,
				'VEHICLE_SALES_EOS',VEHICLE_SALES_EOS,
				'VEHICLE_SALES_SOS',VEHICLE_SALES_SOS,
				'VEHICLE_SALES_VEHICLE_LIFE_CYCLE_IN_MONTHS',VEHICLE_SALES_VEHICLE_LIFE_CYCLE_IN_MONTHS,
				'VEHICLE_SALES_PERIOD_DATE',VEHICLE_SALES_PERIOD_DATE,
				'VEHICLE_SALES_VOLUME',VEHICLE_SALES_VOLUME,
				'FILE_DATE',FILE_DATE,
				'FROM_DB_DATE',FROM_DB_DATE,
				'TO_DB_DATE',TO_DB_DATE
			)
		  )
		), ','
	  ) AS json_data
	FROM (
	  SELECT
		top 5 *,
		ROW_NUMBER() OVER (ORDER BY VEHICLE_ID) AS row_num
	  FROM
		IHS_MOBILITY.FCM_OWNER.FACT_AUTO_LVSF_VOLUME
		where file_date = '2023-12-14'
	) sub
	GROUP BY FLOOR((row_num - 1) / 2000));
------------------------------------------------------------------------------------------------------------------------------------------------
--2)  --Dynamic 
--here column1_param is a table name which dynamicaly pass by user. 
CREATE OR REPLACE PROCEDURE IHS_MOBILITY.FCM_OWNER.PROC_temp_jinesh(column1_param VARCHAR(200))
  RETURNS VARIANT
  LANGUAGE SQL
  COMMENT='user-defined procedure'
  EXECUTE AS OWNER
AS
$$
DECLARE
    SF_DATASET VARCHAR(200);
    SF_TABLE_NAME VARCHAR(200);
    N6_NAMESPACE VARCHAR(200);
    N6_ENTITY VARCHAR(200);
    N6_BATCH_ID VARIANT;
    N6_BATCH_RUN_ID VARIANT;
    N6_MAPPING VARIANT;
    json_str VARIANT;
    sql_stmt varchar(5000);
    jsonFields VARIANT;
    
BEGIN
  SELECT SF_DATASET, SF_TABLE_NAME, N6_NAMESPACE, N6_ENTITY, N6_BATCH_ID, N6_BATCH_RUN_ID, N6_MAPPING
  INTO :SF_DATASET, :SF_TABLE_NAME, :N6_NAMESPACE, :N6_ENTITY, :N6_BATCH_ID, :N6_BATCH_RUN_ID, :N6_MAPPING
  FROM N6_DATA_DICTIONARY_MAPPING
  WHERE SF_TABLE_NAME = :column1_param;

  json_str:=N6_MAPPING;
    
  jsonFields:=(SELECT LISTAGG(''''||VALUE:"n6_field_name"||'''' || ','||VALUE:"sf_field_name",',')
                 FROM (SELECT N6_MAPPING 
                 FROM N6_DATA_DICTIONARY_MAPPING 
                 WHERE SF_TABLE_NAME = :column1_param) j,
                 LATERAL FLATTEN(j.N6_MAPPING));
    
    sql_stmt := 'insert into temp_jsonData (raw_data) SELECT parse_json(''['' || json_data || '']'') FROM (SELECT ARRAY_TO_STRING (ARRAY_AGG (TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(''batch'', OBJECT_CONSTRUCT_KEEP_NULL(''id'', '''||N6_BATCH_ID||'''),''batchRun'', OBJECT_CONSTRUCT_KEEP_NULL(''id'', '''||N6_BATCH_RUN_ID||'''),'||jsonFields||'))), '','') AS json_data FROM (SELECT top 5 *,ROW_NUMBER() OVER (ORDER BY 1) AS row_num FROM ' || column1_param || ') sub GROUP BY FLOOR((row_num - 1) / 2000))';

EXECUTE IMMEDIATE :sql_stmt;
 
  RETURN sql_stmt;
       
END;
$$;
------------------------------------------------------------------------------------------------------------------------------------------------
--output : -- 
--  CALL PROC_temp_jinesh('FACT_AUTO_LVSF_VOLUME');
-- output be like 
[
  {
    "batch": {
      "id": "LVSF_Volume"
    },
    "batchRun": {
      "id": "LVSF_Volume_2024-05-01_1701446400000"
    },
    "fileDate": "2023-10-17",
    "fromDbDate": "2023-11-24 06:20:49.221",
    "id": 5913,
    "salesIso2CountryCode": "PT",
    "toDbDate": "2023-12-15 05:33:04.451",
    "vehicleSalesEos": null,
    "vehicleSalesForecastPublishedDate": null,
    "vehicleSalesLastActualDate": null,
    "vehicleSalesMonthsInMarket": 0,
    "vehicleSalesPeriodDate": "2033-01-01",
    "vehicleSalesRegionalOrigin": "Import",
    "vehicleSalesSos": null,
    "vehicleSalesVehicleLifeCycleInMonths": 128,
    "vehicleSalesVolume": 48
  },
  ... 
  --till end of data
]
