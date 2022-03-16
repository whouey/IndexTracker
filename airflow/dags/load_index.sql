BEGIN;

CREATE TEMP TABLE tmp_vars AS SELECT 'TAIEX' AS name, 'TW' AS market;

INSERT INTO index (name, market)
    SELECT t.name, t.market 
    FROM tmp_vars t LEFT JOIN index i on i.name=t.name and i.market=t.market
    WHERE i.id IS NULL;

INSERT INTO index_history (index_id, scale, datetime, price_open, price_high, price_low, price_close, price_mean, price_std) 
    SELECT i.id, stg.*
    FROM  staging_{{ params.subject }}_{{ ds_underscore(ds) }} stg CROSS JOIN (SELECT id FROM index WHERE index.name='TAIEX' AND index.market='TW') AS i;

COMMIT;