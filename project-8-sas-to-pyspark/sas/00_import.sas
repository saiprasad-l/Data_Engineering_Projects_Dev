/*--------------------------------------------------------------------------
  00_import.sas  -  stage the LendingClub sample as WORK.LOANS

  Every other program in this folder reads WORK.LOANS. On Databricks the
  equivalent is reading the same CSV from a Volume with an explicit schema
  (src/load.py) - PROC IMPORT's type guessing is replaced, not translated.
--------------------------------------------------------------------------*/
%let root    = /home/&sysuserid/sas2databricks;
%let datadir = &root/data;

proc import datafile="&datadir/loans_sample.csv"
            out=work.loans
            dbms=csv
            replace;
    guessingrows=max;
run;
