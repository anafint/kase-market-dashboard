CREATE TABLE exchanges (
    exchange_id     SERIAL PRIMARY KEY,
    ticker          VARCHAR(20) NOT NULL,
    name            VARCHAR(100) NOT NULL,
    country         VARCHAR(50) NOT NULL,
    region          VARCHAR(50) NOT NULL,
    category        VARCHAR(30) NOT NULL
);

CREATE TABLE market_prices (
    price_id        SERIAL PRIMARY KEY,
    exchange_id     INT REFERENCES exchanges(exchange_id),
    date            DATE NOT NULL,
    open            NUMERIC(18,6),
    high            NUMERIC(18,6),
    low             NUMERIC(18,6),
    close           NUMERIC(18,6) NOT NULL,
    volume          BIGINT,
    UNIQUE(exchange_id, date)
);

CREATE TABLE fx_rates (
    fx_id           SERIAL PRIMARY KEY,
    currency_pair   VARCHAR(10) NOT NULL,
    country         VARCHAR(50) NOT NULL,
    date            DATE NOT NULL,
    rate            NUMERIC(18,6) NOT NULL,
    UNIQUE(currency_pair, date)
);

CREATE TABLE macro_indicators (
    macro_id        SERIAL PRIMARY KEY,
    country         VARCHAR(50) NOT NULL,
    indicator_code  VARCHAR(50) NOT NULL,
    indicator_name  VARCHAR(100) NOT NULL,
    year            INT NOT NULL,
    value           NUMERIC(24,6),
    UNIQUE(country, indicator_code, year)
);

CREATE TABLE derived_metrics (
    metric_id       SERIAL PRIMARY KEY,
    exchange_id     INT REFERENCES exchanges(exchange_id),
    date            DATE NOT NULL,
    daily_return    NUMERIC(10,6),
    rolling_vol_30  NUMERIC(10,6),
    rolling_vol_90  NUMERIC(10,6),
    rolling_ret_30  NUMERIC(10,6),
    drawdown        NUMERIC(10,6),
    UNIQUE(exchange_id, date)
);