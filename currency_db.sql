CREATE TABLE IF NOT EXISTS currency_rates (
    date DATE NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    currency_name VARCHAR(100) NOT NULL,
    nominal INTEGER NOT NULL,
    value DECIMAL(10, 4) NOT NULL,
    rate DECIMAL(10, 6) NOT NULL,
    CONSTRAINT pk_currency_rates PRIMARY KEY (date, currency_code)
);

CREATE INDEX IF NOT EXISTS idx_currency_rates_date ON currency_rates (date);
CREATE INDEX IF NOT EXISTS idx_currency_rates_currency_code ON currency_rates (currency_code);