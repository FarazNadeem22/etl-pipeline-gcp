-- We are now learning and practicing the concept of `MERGE` 
-- Select is a read-only statement and it never touches the actual data where as `MERGE` will update or insert into the data 
-- IMPORTANT clauses and statements:
--	`ON` :  
--		The matching condition. 
--		How a target row and source row are decided to be the same. 
--		In this project if the `symbol` and `date` match then it is the same record. 
--	`WHEN MATCHED THEN UPDATE`: 
--		If target exists then update.
--	`WHEN NOT MATCHED THEN INSERT`:
--		If target not found then create a new row with AKA insert
-- Why one statement instead  of a separate UPDATE + INSERT:
--	Atomicity: An operation is atomic if it happens as a single, indivisible unit. Either it fully completes or none of it happens. 
--		   This mitigates the risk of `race-condition`. See `notes/sql-bigquery.md` for more information on atomicity. 
--	This is quicker
--	What is `race-condition`: See `notes/sql-bigquery.md` for more clarity on what `race-condition` is.  

MERGE `clean.daily_prices` AS target

USING (
    SELECT  symbol, date, close_price, volume
    FROM  `staging.daily_prices_raw`
    WHERE symbol IN ('AAPL', 'APLD', 'VOO', 'TMUS', 'JEPQ', 'RIVN', 'QQQ', 'SMCI', 'DUOL')
) AS source

ON target.symbol = source.symbol AND target.date = source.date

WHEN MATCHED THEN UPDATE SET 
    close_price = source.close_price,
    volume   = source.volume

WHEN NOT MATCHED THEN INSERT (
    symbol, 
    date, 
    close_price, 
    volume) 
VALUES (
    source.symbol,
    source.date,
    source.close_price,
    source.volume
);