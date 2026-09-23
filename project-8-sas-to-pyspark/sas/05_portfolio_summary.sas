/*--------------------------------------------------------------------------
  05_portfolio_summary.sas  -  portfolio summary by grade and tenure
  Pattern: PROC MEANS with CLASS and OUTPUT statistics

  Loan count, funded dollars and rate/DTI statistics by grade and
  employment length.

  TRAP: PROC MEANS DROPS rows whose CLASS variable is missing (unless the
        MISSING option is given). emp_length is missing on ~6.5% of loans.
        Spark's groupBy keeps NULL as its own group, so a direct
        translation reports an extra "null" row per grade and totals that
        no longer tie to the SAS report.

  TRAP: N(dti) counts NON-MISSING dti values; _FREQ_ counts rows. They map
        to F.count("dti") and F.count("*") respectively - not interchangeable.
--------------------------------------------------------------------------*/
proc means data=work.loans noprint nway;
    class grade emp_length;
    var funded_amnt int_rate dti;
    output out=work.portfolio_summary(drop=_type_ rename=(_freq_=n_loans))
           sum(funded_amnt) = total_funded
           mean(int_rate)   = avg_rate
           min(int_rate)    = min_rate
           max(int_rate)    = max_rate
           n(dti)           = n_dti
           mean(dti)        = avg_dti;
run;
