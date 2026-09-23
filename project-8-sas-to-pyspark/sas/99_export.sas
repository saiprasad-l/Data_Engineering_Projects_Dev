/*--------------------------------------------------------------------------
  99_export.sas  -  write each program's output to expected/<dataset>.csv

  This is the real-SAS answer key the migration is checked against. Two
  details matter for a fair comparison:
    - numbers are written with BEST32. so nothing is rounded on the way out
      (PROC EXPORT's default would cut avg_rate to ~8 significant digits)
    - dates are written as YYYY-MM-DD, missing values as empty fields
--------------------------------------------------------------------------*/
%let outdir = &root/expected;
options dlcreatedir;
libname _out "&outdir";
libname _out clear;

%macro has_var(ds, var);
    %local dsid pos rc;
    %let dsid = %sysfunc(open(&ds));
    %let pos  = %sysfunc(varnum(&dsid, &var));
    %let rc   = %sysfunc(close(&dsid));
    &pos
%mend has_var;

%macro export(ds);
    data work._export;
        set work.&ds;
        format _numeric_ best32.;
        %if %has_var(work.&ds, issue_d) > 0 %then %do;
            format issue_d yymmdd10.;
        %end;
    run;

    proc export data=work._export
                outfile="&outdir/&ds..csv"
                dbms=csv
                replace;
    run;
%mend export;

options missing=' ';
%export(loan_risk)
%export(state_running)
%export(grade_seq)
%export(grade_mom)
%export(portfolio_summary)
%export(loans_region)
%export(amortization)
options missing='.';
