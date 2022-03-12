BEGIN;

DECLARE table_name TEXT := {{ params.table_name }};

INSERT INTO index (name, market) VALUES ('TAIEX', 'TW') ON CONFLICT DO NOTHING;

INSERT INTO index_history (index_id, scale, datetime, price_open, price_high, price_low, price_close, price_mean, price_std) 
    SELECT i.id, stg.*
    FROM table_name stg CROSS JOIN (SELECT id FROM index WHERE index.name='TAIEX' AND index.market='TW') AS i;

COMMIT;