
CREATE OR REPLACE TABLE column_combination_distinct_counts (
    combination_name STRING,
    distinct_count INTEGER
);

CREATE OR REPLACE PROCEDURE find_min_distinct_combination()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Function to generate combinations of columns
    function getCombinations(arr, k) {
        var i, subI, ret = [], sub, next;
        for (i = 0; i < arr.length; i++) {
            if (k === 1) {
                ret.push([arr[i]]);
            } else {
                sub = getCombinations(arr.slice(i + 1, arr.length), k - 1);
                for (subI = 0; subI < sub.length; subI++) {
                    next = sub[subI];
                    next.unshift(arr[i]);
                    ret.push(next);
                }
            }
        }
        return ret;
    }

    // Query to get the column names from the target table
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
    snowflake.createStatement({sqlText: `DELETE FROM column_combination_distinct_counts`}).execute();

    // Variables to track the minimum distinct count and corresponding combination
    var minDistinctCount = Infinity;
    var minDistinctCombination = [];

    // Generate combinations of columns and calculate distinct count for each combination
    for (var r = 1; r <= columns.length; r++) {
        var combinations = getCombinations(columns, r);
        for (var j = 0; j < combinations.length; j++) {
            var combination = combinations[j];
            var columnsStr = combination.join(', ');
            var sql = `
                SELECT COUNT(DISTINCT ${columnsStr}) AS distinct_count
                FROM ${tableName}
            `;
            var stmt = snowflake.createStatement({sqlText: sql});
            var result = stmt.execute();
            result.next();
            var distinctCount = result.getColumnValue(1);
            
            // Store the combination if it has the minimum distinct count found so far
            if (distinctCount < minDistinctCount) {
                minDistinctCount = distinctCount;
                minDistinctCombination = combination;
            }
            
            // Insert into results table
            var insertSQL = `INSERT INTO column_combination_distinct_counts (combination_name, distinct_count) 
                             VALUES ('${combination.join(', ')}', ${distinctCount})`;
            snowflake.createStatement({sqlText: insertSQL}).execute();
        }
    }

    return 'Minimum distinct count combination found and results inserted into column_combination_distinct_counts table';
$$;


CALL find_min_distinct_combination();
