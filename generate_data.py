"""Creates the synthetic AML (anti-money-laundering) transactions file.

The data is FAKE and generated with a fixed random seed, so you get the same file every time.
We inject a known number of each defect so the pipeline's results can be checked exactly:

    1,000 original rows
  +    25 exact duplicate transaction IDs   -> 1,025 raw rows
  -    25 duplicates removed                -> rule 1
  -    20 rows with a missing customer_id   -> rule 2
  -    18 rows with a negative amount       -> rule 3
  -    20 rows with an inconsistent date    -> rule 4
  = 83 rejected, 942 clean

Run:  python generate_data.py     (creates data/transactions_raw.csv)
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 42
N_BASE = 1000
N_DUPLICATES, N_MISSING_CUSTOMER, N_NEGATIVE, N_BAD_DATE = 25, 20, 18, 20

OUT = Path(__file__).parent / "data" / "transactions_raw.csv"
COLUMNS = ["transaction_id", "customer_id", "amount", "currency",
           "transaction_date", "settlement_date", "country", "channel"]


def build_rows():
    rng = random.Random(SEED)
    start = date(2026, 1, 1)
    rows = []
    for i in range(1, N_BASE + 1):
        tx_date = start + timedelta(days=rng.randint(0, 200))
        rows.append({
            "transaction_id": f"TX{i:06d}",
            "customer_id": f"CUST{rng.randint(1, 300):04d}",
            "amount": round(rng.uniform(5, 15000), 2),
            "currency": rng.choice(["PLN", "EUR", "USD", "GBP"]),
            "transaction_date": tx_date.isoformat(),
            "settlement_date": (tx_date + timedelta(days=rng.randint(0, 3))).isoformat(),
            "country": rng.choice(["PL", "DE", "GB", "US", "AE", "IN"]),
            "channel": rng.choice(["ONLINE", "BRANCH", "ATM", "WIRE"]),
        })

    # Pick different rows for each defect so one row never has two problems.
    chosen = rng.sample(range(N_BASE), N_MISSING_CUSTOMER + N_NEGATIVE + N_BAD_DATE + N_DUPLICATES)
    missing_idx = chosen[:N_MISSING_CUSTOMER]
    negative_idx = chosen[N_MISSING_CUSTOMER:N_MISSING_CUSTOMER + N_NEGATIVE]
    bad_date_idx = chosen[N_MISSING_CUSTOMER + N_NEGATIVE:N_MISSING_CUSTOMER + N_NEGATIVE + N_BAD_DATE]
    duplicate_idx = chosen[-N_DUPLICATES:]

    for i in missing_idx:
        rows[i]["customer_id"] = rng.choice(["", None])        # empty text or truly missing
    for i in negative_idx:
        rows[i]["amount"] = -round(rng.uniform(5, 5000), 2)
    for i in bad_date_idx:
        tx = date.fromisoformat(rows[i]["transaction_date"])
        rows[i]["settlement_date"] = (tx - timedelta(days=rng.randint(1, 10))).isoformat()  # settled BEFORE it happened

    # Duplicates are copies of otherwise-clean rows; they are appended at the end.
    duplicates = [dict(rows[i]) for i in duplicate_idx]
    # (the original of a duplicated row must itself be clean, otherwise the counts above would not add up)
    clean_originals = set(range(N_BASE)) - set(missing_idx) - set(negative_idx) - set(bad_date_idx)
    assert all(i in clean_originals for i in duplicate_idx)
    return rows + duplicates


def main():
    rows = build_rows()
    OUT.parent.mkdir(exist_ok=True)
    with OUT.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {OUT}")


if __name__ == "__main__":
    main()
