-- SAS: RETAIN — running state carried across rows within a BY group
--
--   data out;
--     set loans;
--     by member_id date;
--     retain running_paid 0;
--     if first.member_id then running_paid = 0;
--     running_paid = running_paid + payment_amt;
--   run;
--
-- The DATA step walks rows in order and remembers the previous value.
-- Spark SQL has no loop; the set-based equivalent is a windowed cumulative sum.

SELECT
    member_id,
    date,
    payment_amt,
    SUM(payment_amt) OVER (
        PARTITION BY member_id
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_paid
FROM loans;

-- Note: `ROWS` not `RANGE`. With RANGE, ties on `date` collapse into one frame
-- and every tied row gets the group total — which silently disagrees with SAS,
-- where each row increments separately. This is the single most common way a
-- RETAIN migration produces plausible-but-wrong numbers.
