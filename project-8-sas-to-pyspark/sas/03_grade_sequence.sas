/*--------------------------------------------------------------------------
  03_grade_sequence.sas  -  loan sequence within grade
  Pattern: BY-group FIRST. / LAST. and the sum statement

  Numbers the loans within each grade in issue order and flags the first
  and last loan of each grade.

  TRAP: `loan_seq + 1;` is a SUM STATEMENT. It silently implies RETAIN and
        treats missing as 0. Read as an ordinary expression it looks like
        "loan_seq plus one" per row, which is 1 everywhere.

  TRAP: FIRST./LAST. are not columns - they are flags computed from the
        neighbouring rows in sort order. In Spark they become
        row_number() == 1 and row_number() == count() over the BY group.
--------------------------------------------------------------------------*/
proc sort data=work.loans out=work.loans_by_grade;
    by grade issue_d id;
run;

data work.grade_seq;
    set work.loans_by_grade;
    by grade;

    if first.grade then loan_seq = 0;
    loan_seq + 1;

    is_first = first.grade;
    is_last  = last.grade;

    keep grade issue_d id loan_seq is_first is_last;
run;
