/*--------------------------------------------------------------------------
  01_risk_tier.sas  -  credit policy tiering
  Pattern: IF / THEN / ELSE, boolean flags

  Assigns each loan a policy risk tier from FICO and DTI, and flags high
  revolving utilisation and bad outcomes.

  TRAP: SAS treats a numeric missing as SMALLER than every number, so
        `dti < 20` is TRUE when dti is missing. Spark's NULL makes the same
        comparison NULL (not true), so a loan with missing DTI silently
        drops from LOW to HIGH. 12 loans in the sample hit exactly this.

  TRAP: Without the LENGTH statement SAS sizes risk_tier from the first
        assignment it compiles ('LOW' -> $3) and 'MEDIUM' is truncated to
        'MED'. Spark strings have no fixed length, so a faithful migration
        must decide which behaviour is the intended one.
--------------------------------------------------------------------------*/
data work.loan_risk;
    set work.loans;
    length risk_tier $6;

    if fico_range_low >= 740 and dti < 20 then risk_tier = 'LOW';
    else if fico_range_low >= 680 and dti < 35 then risk_tier = 'MEDIUM';
    else risk_tier = 'HIGH';

    if revol_util > 90 then high_util = 1;
    else high_util = 0;

    is_bad = (loan_status in ('Charged Off', 'Default'));

    keep id grade fico_range_low dti revol_util loan_status
         risk_tier high_util is_bad;
run;
