/*--------------------------------------------------------------------------
  10_amortization.sas  -  monthly amortization schedule
  Pattern: DO-loop OUTPUT (row explosion) + self-referential RETAIN

  Expands each Q4-2018 grade-A loan into its scheduled monthly payments and
  walks the outstanding balance forward month by month.

  TRAP: This month's balance depends on LAST month's COMPUTED balance, not
        on an input column. No window function can express that - a window
        can only look at inputs. Rounding to cents and the floor at zero
        also rule out a closed-form formula. In PySpark the honest answer is
        to group by loan and walk the months in order (applyInPandas).

  TRAP: SAS ROUND(x, 0.01) rounds half away from zero and applies a small
        fuzz so 2.675 (really 2.67499999... in binary) becomes 2.68.
        Python's round() is banker's rounding and Spark's F.round works on
        the exact binary value - both can differ from SAS by a cent, and a
        cent compounds across a 36-month schedule.
--------------------------------------------------------------------------*/
data work.schedule;
    set work.loans(where=(issue_d >= '01OCT2018'd and grade = 'A'));
    monthly_rate = int_rate / 100 / 12;

    do month_num = 1 to term;
        output;
    end;

    keep id month_num term monthly_rate installment funded_amnt;
run;

proc sort data=work.schedule;
    by id month_num;
run;

data work.amortization;
    set work.schedule;
    by id;
    retain balance;

    if first.id then balance = funded_amnt;

    interest  = round(balance * monthly_rate, 0.01);
    principal = round(installment - interest, 0.01);
    balance   = max(0, round(balance - principal, 0.01));

    keep id month_num interest principal balance;
run;
