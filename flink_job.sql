-- StreamShield AI: Flink SQL Architecture

-- 1. Create Materialized Table for Fraud Decisions
CREATE MATERIALIZED TABLE `fraud-decisions-mat` (
    `transaction_id` STRING NOT NULL,
    `customer_id` STRING,
    `amount` DOUBLE,
    `currency` STRING,
    `txn_country` STRING,
    `home_country` STRING,
    `device_id` STRING,
    `amount_risk` INT,
    `location_risk` INT,
    `device_risk` INT,
    `total_risk_score` INT,
    `decision` STRING,
    PRIMARY KEY (`transaction_id`) NOT ENFORCED
) WITH (
    'changelog.mode' = 'upsert'
) AS
SELECT 
    t.transaction_id,
    t.customer_id,
    t.amount,
    t.currency,
    t.country AS txn_country,
    c.country AS home_country,
    t.device_id,
    -- Amount Risk Rule
    CASE WHEN t.amount > 5000 THEN 30 ELSE 0 END AS amount_risk,
    -- Location Risk Rule
    CASE WHEN t.country != c.country THEN 20 ELSE 0 END AS location_risk,
    -- Device Risk Rule
    CASE WHEN t.device_id LIKE '%NEW%' THEN 25 ELSE 0 END AS device_risk,
    -- Total Score
    (
        (CASE WHEN t.amount > 5000 THEN 30 ELSE 0 END) +
        (CASE WHEN t.country != c.country THEN 20 ELSE 0 END) +
        (CASE WHEN t.device_id LIKE '%NEW%' THEN 25 ELSE 0 END)
    ) AS total_risk_score,
    -- Deterministic Decision Logic
    CASE 
        WHEN (
            (CASE WHEN t.amount > 5000 THEN 30 ELSE 0 END) +
            (CASE WHEN t.country != c.country THEN 20 ELSE 0 END) +
            (CASE WHEN t.device_id LIKE '%NEW%' THEN 25 ELSE 0 END)
        ) >= 70 THEN 'BLOCK'
        WHEN (
            (CASE WHEN t.amount > 5000 THEN 30 ELSE 0 END) +
            (CASE WHEN t.country != c.country THEN 20 ELSE 0 END) +
            (CASE WHEN t.device_id LIKE '%NEW%' THEN 25 ELSE 0 END)
        ) >= 40 THEN 'REVIEW'
        ELSE 'APPROVE'
    END AS decision
FROM `transactions` t
LEFT JOIN `customer-profile` c ON t.customer_id = c.userid;
