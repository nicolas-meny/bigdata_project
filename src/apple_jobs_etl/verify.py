import argparse
import csv
import re
from collections import Counter


def main() -> None:
    parser = argparse.ArgumentParser(description="Independent verification of Gold layer metrics")
    parser.add_argument("--raw-csv", default="/tmp/medallion_work/bronze/apple_jobs.csv")
    parser.add_argument("--gold-csv", default="/tmp/medallion_work/gold/country_skills_summary.csv")
    args = parser.parse_args()

    seen_rows = set()
    country_counts = Counter()
    python_counts = Counter()

    with open(args.raw_csv, mode="r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = tuple(row[col] for col in reader.fieldnames)
            if key in seen_rows:
                continue
            seen_rows.add(key)

            loc = re.sub(r"\s+", " ", (row.get("location") or "").strip())
            country = loc.split(",")[-1].strip() if loc else ""
            country_counts[country] += 1

            full_text = " ".join(
                (row.get(c) or "")
                for c in ["title", "minimum_qual", "preferred_qual", "responsibilities", "education&experience"]
            ).lower()
            if re.search(r"\bpython\b", full_text):
                python_counts[country] += 1

    with open(args.gold_csv, mode="r", encoding="utf-8", newline="") as f:
        gold_rows = list(csv.DictReader(f))

    assert len(gold_rows) == len(country_counts), (
        f"Country count mismatch: Gold={len(gold_rows)} vs Independent={len(country_counts)}"
    )

    for g in gold_rows:
        c = g["country"]
        assert int(g["total_jobs"]) == country_counts[c], f"Mismatch total_jobs for {c}"
        assert int(g["python_jobs"]) == python_counts[c], f"Mismatch python_jobs for {c}"

    top = gold_rows[0]
    print(f"[VERIFIED] All {len(gold_rows)} countries match 100% between raw CSV and Gold output.")
    print(
        f"[VERIFIED] Top country: {top['country']} -> total_jobs={top['total_jobs']}, "
        f"python_jobs={top['python_jobs']} ({top['python_pct']}%)"
    )


if __name__ == "__main__":
    main()