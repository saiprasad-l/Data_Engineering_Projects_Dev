/*--------------------------------------------------------------------------
  07_branch_region.sas  -  attach servicing region to each loan
  Pattern: MERGE ... BY with IN= flags against a reference table

  The lender services loans through regional branch networks that do not
  cover every state. Loans in uncovered states are flagged UNMAPPED.

  TRAP: MERGE is not a SQL join. With IN= flags, `if in_loans;` gives a
        left join; dropping that line gives a full outer join; many-to-many
        BY values are matched pairwise, NOT as a Cartesian product.

  TRAP: When both inputs carry the same non-BY column, MERGE silently keeps
        the value from the dataset listed LAST. A SQL join forces you to
        choose explicitly (COALESCE / table prefix) - the migration must
        encode SAS's implicit choice, not guess.

  TRAP: Both inputs must be sorted BY the merge key. In Spark the join does
        not need (or benefit from) the sort - the PROC SORTs are dropped.
--------------------------------------------------------------------------*/
data work.branch_region;
    length addr_state $2 region $12;
    input addr_state $ region $;
    datalines;
CT Northeast
MA Northeast
NJ Northeast
NY Northeast
PA Northeast
RI Northeast
DE MidAtlantic
MD MidAtlantic
VA MidAtlantic
WV MidAtlantic
NC Southeast
SC Southeast
GA Southeast
FL Southeast
AL Southeast
TN Southeast
KY Southeast
MS Southeast
IL Midwest
IN Midwest
IA Midwest
MI Midwest
MN Midwest
MO Midwest
OH Midwest
WI Midwest
KS Midwest
NE Midwest
AR SouthCentral
LA SouthCentral
OK SouthCentral
TX SouthCentral
AZ West
CA West
CO West
NV West
NM West
OR West
UT West
WA West
;
run;

proc sort data=work.loans out=work.loans_by_state;
    by addr_state;
run;

proc sort data=work.branch_region;
    by addr_state;
run;

data work.loans_region;
    merge work.loans_by_state(in=in_loans)
          work.branch_region(in=in_ref);
    by addr_state;

    if in_loans;
    if not in_ref then region = 'UNMAPPED';

    keep id addr_state region funded_amnt;
run;
