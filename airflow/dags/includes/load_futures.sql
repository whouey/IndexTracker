BEGIN;
DO
$$
DECLARE 
    table_name TEXT := {{ params.table_name }};
BEGIN

RAISE NOTICE 'staging table name is specified as %', table_name

INSERT INTO derivative (index_id, name, derivative_type)
    SELECT DISTINCT i.id, stg.contract, 'Futures' 
    FROM table_name stg, (SELECT id FROM index WHERE index.name='TAIEX' AND index.market='TW') AS i
ON CONFLICT DO NOTHING;

INSERT INTO derivative_detail (derivative_id)
    SELECT DISTINCT d.id, stg.expire, stg.expire_code
    FROM table_name stg JOIN (SELECT * FROM derivative WHERE derivative_type='Futures') d ON d.name = stg.contract 
ON CONFLICT DO NOTHING;

INSERT INTO derivative_detail_history
    SELECT dev.id, stg.scale, stg.datetime, stg.price_open, stg.price_high, stg.price_low, stg.price_close, stg.price_mean, stg.price_std, stg.volume 
    FROM table_name stg NATURAL JOIN (
        SELECT dd.id AS id, d.name AS contract, dd.expire AS expire, dd.expire_code AS expire_code 
        FROM derivative_detail dd JOIN derivative d ON dd.derivative_id = d.id WHERE d.derivative_type='Futures') dev

END;
$$
COMMIT;