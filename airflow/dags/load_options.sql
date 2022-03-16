BEGIN;

INSERT INTO derivative (index_id, name, derivative_type) 
	SELECT new_devs.index_id, new_devs.name, new_devs.derivative_type 
	FROM (
  		SELECT DISTINCT i.id AS index_id, stg.contract AS name, 'Options' AS derivative_type
  		FROM staging_{{ params.subject }}_{{ ds_underscore(ds) }} stg, (SELECT id FROM index WHERE index.name='TAIEX' AND index.market='TW') AS i
    ) as new_devs 
	NATURAL LEFT JOIN derivative d 
	WHERE d.id IS NULL;

INSERT INTO derivative_detail (derivative_id, expire, expire_code, strike_price, option_type) 
    SELECT DISTINCT d.id, stg.expire, stg.expire_code, stg.strike_price, stg.option_type 
    FROM staging_{{ params.subject }}_{{ ds_underscore(ds) }} stg 
        JOIN (SELECT * FROM derivative WHERE derivative_type='Options') d ON d.name = stg.contract 
        LEFT JOIN derivative_detail dd 
            ON dd.derivative_id = d.id 
            AND dd.expire = stg.expire 
            AND dd.expire_code = stg.expire_code 
            AND dd.strike_price = stg.strike_price 
            AND dd.option_type = stg.option_type 
        WHERE dd.id IS NULL;

INSERT INTO derivative_detail_history
    SELECT dev.id, stg.scale, stg.datetime, stg.price_open, stg.price_high, stg.price_low, stg.price_close, stg.price_mean, stg.price_std, stg.volume 
    FROM staging_{{ params.subject }}_{{ ds_underscore(ds) }} stg NATURAL JOIN (
        SELECT 
            dd.id AS id, 
            d.name AS contract, 
            dd.expire AS expire, 
            dd.expire_code AS expire_code, 
            dd.strike_price AS strike_price,
            dd.option_type AS option_type 
        FROM derivative_detail dd 
            JOIN derivative d ON dd.derivative_id = d.id 
        WHERE d.derivative_type='Options') dev;

COMMIT;