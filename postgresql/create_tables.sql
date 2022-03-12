CREATE TABLE index(
    id INT IDENTITY(0, 1) PRIMARY KEY,
    name TEXT NOT NULL,
    market TEXT NOT NULL,
    UNIQUE(name, market)
) DISTSTYLE ALL;

CREATE TABLE index_history(
    index_id INT NOT NULL REFERENCES index,
    scale INT NOT NULL,
    datetime TIMESTAMP NOT NULL,
    price_open NUMERIC(10, 2),
    price_high NUMERIC(10, 2),
    price_low NUMERIC(10, 2),
    price_close NUMERIC(10, 2),
    price_mean NUMERIC(14, 6),
    price_std FLOAT,
    PRIMARY KEY (index_id, scale, datetime)
) DISTSTYLE EVEN SORTKEY(scale, datetime);

CREATE TABLE derivative(
    id INT IDENTITY(0, 1) PRIMARY KEY,
    index_id INT NOT NULL REFERENCES index,
    name TEXT NOT NULL,
    derivative_type TEXT NOT NULL,
    UNIQUE(index_id, name, derivative_type)
) DISTSTYLE ALL;

CREATE TABLE derivative_detail(
    id INT IDENTITY(0, 1) PRIMARY KEY,
    derivative_id INT NOT NULL REFERENCES derivative,
    expire TEXT DEFAULT NULL,
    expire_code TEXT DEFAULT NULL,
    strike_price TEXT DEFAULT NULL,
    option_type TEXT DEFAULT NULL,
    UNIQUE(derivative_id, expire, expire_code, strike_price, option_type)
) DISTSTYLE ALL;

CREATE TABLE derivative_detail_history(
    derivative_detail_id INT NOT NULL REFERENCES derivative_detail,
    scale INT NOT NULL,
    datetime TIMESTAMP NOT NULL,
    price_open NUMERIC(10, 2),
    price_high NUMERIC(10, 2),
    price_low NUMERIC(10, 2), 
    price_close NUMERIC(10, 2),
    price_mean NUMERIC(14, 6),
    price_std FLOAT,
    volume BIGINT,
    PRIMARY KEY(derivative_detail_id, scale, datetime)
) DISTSTYLE EVEN SORTKEY(scale, datetime);