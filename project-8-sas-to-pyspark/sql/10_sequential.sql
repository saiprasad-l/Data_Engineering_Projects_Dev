-- SAS: self-referential RETAIN — row N depends on a COMPUTED value from row N-1.
-- This is the case that does NOT reduce to a window function.
--
--   data out;
--     set accounts;
--     by account_id cycle;
--     retain balance;
--     if first.account_id then balance = opening_balance;
--     else balance = balance * (1 + rate) - payment;   /* depends on prior COMPUTED balance */
--   run;
--
-- A cumulative window can't express this: `balance` is not a lag of an input
-- column, it's a lag of its own output. Two honest options.

-- Option A — recursive CTE (Databricks Runtime 14.3+ / DBSQL).
WITH RECURSIVE walk AS (
    SELECT account_id, cycle, opening_balance AS balance, rate, payment
    FROM accounts
    WHERE cycle = 1

    UNION ALL

    SELECT a.account_id, a.cycle,
           w.balance * (1 + a.rate) - a.payment,
           a.rate, a.payment
    FROM accounts a
    JOIN walk w
      ON a.account_id = w.account_id
     AND a.cycle      = w.cycle + 1
)
SELECT account_id, cycle, balance FROM walk;

-- Option B — closed form, when the recurrence is linear. Expanding
--   b_n = b_0 * Π(1+r) - Σ(payment_i * Π_{j>i}(1+r_j))
-- lets it become windowed products/sums. Faster, but only some recurrences
-- have a closed form, and it is easy to get subtly wrong.

-- Option C — PySpark. When neither of the above fits, this is the right answer,
-- not a failure. See notebooks/10_sequential.py: partition by account_id,
-- sort within partition, and fold. Correctness beats staying in SQL.
