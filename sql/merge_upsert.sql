-- Idempotent upsert of staged daily price rows into the clean target table.
--
-- Why MERGE instead of INSERT: if this pipeline is retried after a partial
-- failure (network blip, task timeout, etc.), a plain INSERT would create
-- duplicate rows for the same (symbol, date). MERGE makes the operation
-- idempotent -- rerunning it against the same staged data produces the same
-- end state every time, which is the property that makes this pipeline safe
-- to retry automatically rather than requiring manual cleanup.

MERGE `your-project.clean.daily_prices` AS target
USING (
  SELECT
    symbol,
    date,
    close_price,
    volume,
    CURRENT_TIMESTAMP() AS loaded_at
  FROM `your-project.staging.daily_prices_raw`
  WHERE date = @execution_date   -- parameterized by the DAG's execution date
) AS source
ON target.symbol = source.symbol AND target.date = source.date

WHEN MATCHED THEN
  UPDATE SET
    close_price = source.close_price,
    volume = source.volume,
    loaded_at = source.loaded_at

WHEN NOT MATCHED THEN
  INSERT (symbol, date, close_price, volume, loaded_at)
  VALUES (source.symbol, source.date, source.close_price, source.volume, source.loaded_at);
