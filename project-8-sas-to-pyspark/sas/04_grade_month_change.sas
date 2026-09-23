/*--------------------------------------------------------------------------
  04_grade_month_change.sas  -  month-over-month funded volume by grade
  Pattern: PROC SUMMARY feeding a DATA step with LAG / DIF

  Monthly funded volume per grade, then the change versus the prior month.

  TRAP: LAG() is not "the previous row". It is a QUEUE that is only updated
        when the LAG call executes. Written as
            if not first.grade then prev_funded = lag(funded);
        the queue skips the first row of every grade and returns stale
        values. The correct idiom (below) calls LAG on every row and blanks
        the result on the first row of the group - which is what
        F.lag(...).over(partitionBy(grade)) does natively.

  TRAP: PROC SUMMARY output is sorted by the CLASS variables, and the
        DATA step's BY grade silently depends on that. Spark output has no
        order, so the window must order by issue_d explicitly.
--------------------------------------------------------------------------*/
proc summary data=work.loans nway;
    class grade issue_d;
    var funded_amnt;
    output out=work.grade_month(drop=_type_ rename=(_freq_=n_loans))
           sum=funded;
run;

data work.grade_mom;
    set work.grade_month;
    by grade;

    prev_funded = lag(funded);
    funded_chg  = dif(funded);

    if first.grade then do;
        prev_funded = .;
        funded_chg  = .;
    end;

    keep grade issue_d n_loans funded prev_funded funded_chg;
run;
