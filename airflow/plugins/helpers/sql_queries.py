create_staging_table_index = \
"""
CREATE TABLE {table_name}(
    scale INT,
    datetime TIMESTAMP,
    price_open NUMERIC(10, 2),
    price_high NUMERIC(10, 2),
    price_low NUMERIC(10, 2),
    price_close NUMERIC(10, 2),
    price_mean NUMERIC(14, 6),
    price_std FLOAT
);
"""

create_staging_table_etf = \
"""
CREATE TABLE {table_name}(
    contract TEXT,
    scale INT,
    datetime TIMESTAMP,
    price_open NUMERIC(10, 2),
    price_high NUMERIC(10, 2),
    price_low NUMERIC(10, 2),
    price_close NUMERIC(10, 2),
    price_mean NUMERIC(14, 6),
    price_std FLOAT,
    volume BIGINT
);
"""

create_staging_table_futures = \
"""
CREATE TABLE {table_name}(
    contract TEXT,
    expire TEXT,
    expire_code TEXT,
    scale INT,
    datetime TIMESTAMP,
    price_open NUMERIC(10, 2),
    price_high NUMERIC(10, 2),
    price_low NUMERIC(10, 2),
    price_close NUMERIC(10, 2),
    price_mean NUMERIC(14, 6),
    price_std FLOAT,
    volume BIGINT
);
"""

create_staging_table_options = \
"""
CREATE TABLE {table_name}(
    contract TEXT,
    expire TEXT,
    expire_code TEXT,
    strike_price TEXT,
    option_type TEXT,
    scale INT,
    datetime TIMESTAMP,
    price_open NUMERIC(10, 2),
    price_high NUMERIC(10, 2),
    price_low NUMERIC(10, 2),
    price_close NUMERIC(10, 2),
    price_mean NUMERIC(14, 6),
    price_std FLOAT,
    volume BIGINT
);
"""
create_staging_table = {
    'index': create_staging_table_index,
    'etf': create_staging_table_etf,
    'futures': create_staging_table_futures,
    'options': create_staging_table_options,
}


data_quality_check_index = \
"""
SELECT 
    SUM(CASE WHEN scale IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN datetime IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_open IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_high IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_low IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_close IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_mean IS NULL THEN 1 ELSE 0 END)=0 
FROM {table_name}
"""

data_quality_check_etf = \
"""
SELECT 
    SUM(CASE WHEN contract IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN scale IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN datetime IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_open IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_high IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_low IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_close IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_mean IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END)=0 
FROM {table_name}
"""

data_quality_check_futures = \
"""
SELECT 
    SUM(CASE WHEN contract IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN expire IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN expire_code IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN scale IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN datetime IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_open IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_high IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_low IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_close IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_mean IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END)=0 
FROM {table_name}
"""

data_quality_check_options = \
"""
SELECT 
    SUM(CASE WHEN contract IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN expire IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN expire_code IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN strike_price IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN option_type IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN scale IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN datetime IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_open IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_high IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_low IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_close IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN price_mean IS NULL THEN 1 ELSE 0 END)=0, 
    SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END)=0 
FROM {table_name}
"""

data_quality_check = \
{
    'index': data_quality_check_index,
    'etf': data_quality_check_etf,
    'futures': data_quality_check_futures,
    'options': data_quality_check_options,
}