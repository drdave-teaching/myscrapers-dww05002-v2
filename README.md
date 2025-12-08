# craigslist-scraper
Let's see how good the GCP instructions are!

Initially, this ran everything that was on the `myscrapers` repo - but now it has been enhanced to:
1) update the fields extracted by the LLM
2) materialize the LLM data
3) sends the predictions from the RegEx/ETL DT model from GCS to GitHub (hosted by GitHub Actions, not by GCP!)

Students will take it a step further to use the LLM-materialized data to build a better, interpretable model and will send outputs to their repo for review.
