/* First I take time to understand the problem fully. There are two tables: Silver.transactions and Mapping.transactions,
which share common columns: Code and System Key. Joining on these columns can create duplicate rows, causing merge conflicts. 

I then will create a view that joins these two tables on the shared columns to see what the merged data looks like.

I next need to identify duplicate rows (not duplicate columns).
I will write a SQL query that shows what happens before the merge, to identify which records will cause the "duplicate transaction id error."

Then I will need to diagnose why these duplicates are occurring by seeinng all of the rows

Next I will need to idenitfy the root of the duplicates by checking if the mapping table has duplicates.
this will help me understand if the issue is with the mapping table or the transactions table.

I will now create a query to remove the duplicates from the mapping table permanently, as this is the root cause of the issue.

Lastly I will connect the merge statement for the nightly process to use the cleaned mapping table to avoid future conflicts.


In summary, here are the steps I will take:
1. Identify → 2. Diagnose → 3. Find Root Cause → 4. Clean Data → 5. Fix MERGE


CHOSEN SOLUTION: Clean the Mapping table permanently

This solution is best because it addresses the root cause of the duplicates. By removing duplicates from the Mapping.transactions table,
we ensure that future joins will not produce duplicate rows, thus preventing merge conflicts. This will produce better data quality over time.

 */



/* View that joins Silver.transactions and Mapping.transactions for cleaner code*/
CREATE VIEW vw_TransactionMapping AS
SELECT 
    t.Transaction_Id,
    t.Site,
    t.System_Key,
    t.Code,
    m.Code as Mapping_Code,
    m.MAPPING,
    m.System_Key as Mapping_System_Key
FROM Silver.transactions t
INNER JOIN Mapping.transactions m 
    ON t.Code = m.Code 
    AND t.System_Key = m.System_Key;




/* 1) Identify: Identifying which rows are duplicating after the join and causing the merge conflict */

/* Before the nightly merge process, counts how many times each key appears, only shows groups with more than 1 row(the duplicates),
and flags the duplicates with duplicate_count */
SELECT 
    Transaction_Id,
    Site,
    System_Key,
    COUNT(*) as duplicate_count
FROM vw_TransactionMapping
GROUP BY Transaction_Id, Site, System_Key
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, Transaction_Id, Site;



/* 2) Diagnose:Get all rows and columns for the duplicate records to diagnose why duplicates are occurring */
SELECT *
FROM vw_TransactionMapping
WHERE (Transaction_Id, Site, System_Key) IN (
    SELECT Transaction_Id, Site, System_Key
    FROM vw_TransactionMapping
    GROUP BY Transaction_Id, Site, System_Key
    HAVING COUNT(*) > 1
)
ORDER BY Transaction_Id, Site, System_Key, Code;


/* 3) Find Root Cause:Checks mapping table for duplicates to find the root cause and shows exactly which values are conflicting(all_mappings) */
SELECT 
    Code,
    System_Key,
    COUNT(*) as mapping_count,
    STRING_AGG(MAPPING, ', ') as all_mappings
FROM Mapping.transactions
GROUP BY Code, System_Key
HAVING COUNT(*) > 1
ORDER BY mapping_count DESC;



/* 4) Clean Data: Cleans the mapping table by removing duplicate mappings, keeping only one per Code+System_Key combination */
WITH MappingToKeep AS (
    /* Identify which MAPPING to keep for each (Code, System_Key) combination */
    SELECT 
        Code,
        System_Key,
        MIN(MAPPING) as MAPPING  -- Keeps first alphabetically (since business logic isn't specified)
    FROM Mapping.transactions
    GROUP BY Code, System_Key
)
DELETE m
FROM Mapping.transactions m
LEFT JOIN MappingToKeep k 
    ON m.Code = k.Code 
    AND m.System_Key = k.System_Key 
    AND m.MAPPING = k.MAPPING
WHERE k.Code IS NULL;  -- Delete rows that does not match the row to keep

/* After cleaning the mapping table, verify no duplicates remain */
SELECT 
    Code,
    System_Key,
    COUNT(*) as mapping_count
FROM Mapping.transactions
GROUP BY Code, System_Key
HAVING COUNT(*) > 1;




/* 5) Fix MERGE: Corrected MERGE statement that uses the cleaned mapping table to avoid duplicates.
Runs nightly */
MERGE Silver.transactions AS target
USING (
    /* Source query: Creates the dataset to merge FROM
    This joins transactions with the cleaned mapping table */
    SELECT 
        t.Transaction_Id,
        t.Site,
        t.System_Key,
        t.Code,
        m.MAPPING  -- Brings in the MAPPING column from the mapping table
    FROM Silver.transactions t
    INNER JOIN Mapping.transactions m 
        ON t.Code = m.Code           -- Join condition 1: Code must match
        AND t.System_Key = m.System_Key  -- Join condition 2: System_Key must match
    /* After cleaning duplicates, this produces one row per (Transaction_Id, Site, System_Key) */
) AS source 
/* Match Criteria: How to determine if a source row already exists in target */
ON target.Transaction_Id = source.Transaction_Id    
    AND target.Site = source.Site                   
    AND target.System_Key = source.System_Key       
    /* If all three match, the row exists, UPDATE
       If they don't match, it's new, INSERT */

/* WHEN MATCHED: The source row exists in the target table */
WHEN MATCHED THEN
    UPDATE SET 
        target.Code = source.Code,        
        target.MAPPING = source.MAPPING   
    /* This refreshes existing records with current mapping information */

/* WHEN NOT MATCHED: The source row does NOT exist in the target table */
WHEN NOT MATCHED THEN
    INSERT (Transaction_Id, Site, System_Key, Code, MAPPING)
    VALUES (source.Transaction_Id, source.Site, source.System_Key, 
            source.Code, source.MAPPING);  
            /* This inserts the new row from source */

/* End of SQL Script */