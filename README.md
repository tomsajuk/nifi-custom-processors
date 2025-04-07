# nifi-custom-processors
Start of some custom processors for use in Apache Nifi

1. Bulk Insert To SQL DB

Modified from PutDatabaseRecord Processor to Pick up a batch of Flowfiles from Queue and create a SINGLE SQL statement to insert all the records in the batch into a table.
The reason behind this processor was to handle the case of Inserting into Clickhouse Database, wherein the current PutDatabaseRecord was creating single query for each FlowFile and executing the queries which was causing a huge load on Clickhosue DB because as the Batch Insert query was considered as single INSERT query by Clickhouse which was making it consume a lot of Memory and CPU during Bulk Inserts of over 500 records. 
Note that this is in case of ReplacingTree Engine with ORDER By Clause intending for merging of records in case of duplicates. The memory and CPU was due to execution of Merge Command for every INSERT query.
