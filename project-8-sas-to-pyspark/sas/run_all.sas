/*--------------------------------------------------------------------------
  run_all.sas  -  run every program and export the results (SAS OnDemand)

  Expects this layout in your SAS home folder:
      ~/sas2databricks/data/loans_sample.csv
      ~/sas2databricks/sas/*.sas
  Writes ~/sas2databricks/expected/*.csv
--------------------------------------------------------------------------*/
%let sasdir = /home/&sysuserid/sas2databricks/sas;

%include "&sasdir/00_import.sas";
%include "&sasdir/01_risk_tier.sas";
%include "&sasdir/02_state_running_total.sas";
%include "&sasdir/03_grade_sequence.sas";
%include "&sasdir/04_grade_month_change.sas";
%include "&sasdir/05_portfolio_summary.sas";
%include "&sasdir/07_branch_region.sas";
%include "&sasdir/10_amortization.sas";
%include "&sasdir/99_export.sas";
