## Idempotence
### Explanation:
In the provided code, idempotence is achieved through the upsert operation in the upsert method. 

Here, the MERGE SQL statement checks if a record already exists in the target table (invoices_bz) by comparing unique keys (e.g., value and timestamp). 

If the record exists and matches, it updates the record to ensure data consistency. 

If it does not exist, it inserts the record as a new entry. 

This prevents duplicate or inconsistent data from being introduced into the table, even if the previous streaming batch failed or was incomplete.