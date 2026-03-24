ALTER TABLE futures_daily_data
DROP INDEX uk_symbol_trade_date,
ADD UNIQUE KEY uk_symbol_trade_date_source (symbol, trade_date, data_source);
