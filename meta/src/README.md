# Meta Ads Pipeline

This folder contains the ETL pipeline for Facebook & Instagram Ads data.

**Flow:**  
API → Transform → BigQuery Load (Daily automation).

---

## Run
1. Add `.env` with API credentials.
2. Run `python src/main.py`.

---

Future updates: async batch jobs, creative performance metrics.
