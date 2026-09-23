/*--------------------------------------------------------------------------
  02_state_running_total.sas  -  cumulative funded volume by state
  Pattern: RETAIN running total within a BY group

  For each state, the running total of funded dollars and loan count in
  issue order - the kind of series that feeds concentration-limit reports.

  TRAP: Many loans share an issue month (up to ~800 per month here). SAS
        adds one row at a time, so tied rows get DIFFERENT running totals.
        A window ordered only by issue_d uses a RANGE frame by default:
        every tied row gets the same total for the whole month. The numbers
        look plausible and are wrong. The window must be ordered by the full
        sort key (issue_d, id) with an explicit ROWS frame.

  TRAP: SAS relies on the physical row order PROC SORT produced. Spark has
        no row order; the migration must carry the sort key into the window.
--------------------------------------------------------------------------*/
proc sort data=work.loans out=work.loans_by_state;
    by addr_state issue_d id;
run;

data work.state_running;
    set work.loans_by_state;
    by addr_state;
    retain cum_funded 0;

    if first.addr_state then do;
        cum_funded = 0;
        cum_loans = 0;
    end;

    cum_funded = cum_funded + funded_amnt;
    cum_loans + 1;

    keep addr_state issue_d id funded_amnt cum_funded cum_loans;
run;
