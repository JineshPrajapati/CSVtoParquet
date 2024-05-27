CREATE OR REPLACE TABLE column_distinct_counts (
    column_name STRING,
    distinct_count INTEGER,
    total_count INTEGER
);
CREATE OR REPLACE PROCEDURE calculate_distinct_counts()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    var sql_command = `SELECT COLUMN_NAME 
                       FROM INFORMATION_SCHEMA.COLUMNS 
                       WHERE TABLE_NAME = 'AUTO_CREDIT_INSIGHT' 
                       ORDER BY ORDINAL_POSITION`;
    
    var stmt = snowflake.createStatement({sqlText: sql_command});
    var result = stmt.execute();
    
    var columns = [];
    while (result.next()) {
        columns.push(result.getColumnValue(1));
    }
    
    var tableName = 'IHS_MOBILITY.FCM_OWNER.AUTO_CREDIT_INSIGHT'; // Replace with your schema and table name

    // Clear existing results in the results table
    snowflake.createStatement({sqlText: `DELETE FROM column_distinct_counts`}).execute();

    // Calculate distinct count and total count for each column and insert into the results table
    columns.forEach(function(column) {
        // Calculate distinct count
        var sqlDistinctCount = `SELECT COUNT(DISTINCT ${column}) AS distinct_count FROM ${tableName}`;
        var stmtDistinctCount = snowflake.createStatement({sqlText: sqlDistinctCount});
        var resultDistinctCount = stmtDistinctCount.execute();
        resultDistinctCount.next();
        var distinctCount = resultDistinctCount.getColumnValue(1);

        // Calculate total count
        var sqlTotalCount = `SELECT COUNT(*) AS total_count FROM ${tableName}`;
        var stmtTotalCount = snowflake.createStatement({sqlText: sqlTotalCount});
        var resultTotalCount = stmtTotalCount.execute();
        resultTotalCount.next();
        var totalCount = resultTotalCount.getColumnValue(1);

        // Insert into results table
        var insertSQL = `INSERT INTO column_distinct_counts (column_name, distinct_count, total_count) 
                         VALUES ('${column}', ${distinctCount}, ${totalCount})`;
        snowflake.createStatement({sqlText: insertSQL}).execute();
    });

    return 'Distinct and total counts inserted into column_distinct_counts table';
$$;


CALL calculate_distinct_counts();


SELECT *
FROM column_distinct_counts
ORDER BY distinct_count DESC;

