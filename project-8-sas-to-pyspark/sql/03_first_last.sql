-- SAS: BY-group processing with FIRST. / LAST. flags
--
--   data out;
--     set loans;
--     by member_id date;
--     if first.member_id then seq = 1; else seq + 1;
--     is_last = last.member_id;
--   run;

SELECT
    member_id,
    date,
    ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY date) AS seq,
    ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY date) = 1                       AS is_first,
    ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY date)
        = COUNT(*)  OVER (PARTITION BY member_id)                                      AS is_last
FROM loans;

-- SAS requires the input to be sorted by the BY variables (PROC SORT first, or
-- it errors). Spark does not — ORDER BY inside the window is sufficient, and
-- there is no global sort requirement. Dropping the PROC SORT is safe.
