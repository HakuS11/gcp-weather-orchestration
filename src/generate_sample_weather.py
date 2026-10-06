import json
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "data" / "sample" / "weather.ndjson"


def generate_records():
    start_date = date(2026, 9, 1)

    cities = [
        ("Chandler", 38.0, 25.0),
        ("Seattle", 22.0, 14.0),
        ("New York", 28.0, 19.0),
    ]

    records = []

    for city, base_max, base_min in cities:
        for day_offset in range(7):
            records.append({
                "city": city,
                "date": (
                    start_date + timedelta(days=day_offset)
                ).isoformat(),
                "temperature_max_c": round(
                    base_max + day_offset * 0.3, 1
                ),
                "temperature_min_c": round(
                    base_min + day_offset * 0.2, 1
                ),
                "precipitation_mm": (
                    2.5 if day_offset % 3 == 0 else 0.0
                ),
            })

    records.append(records[0].copy())

    records.extend([
        {
            "city": None,
            "date": "2026-09-01",
            "temperature_max_c": 30.0,
            "temperature_min_c": 20.0,
            "precipitation_mm": 0.0,
        },
        {
            "city": "Chandler",
            "date": "2026-09-08",
            "temperature_max_c": 20.0,
            "temperature_min_c": 30.0,
            "precipitation_mm": 0.0,
        },
        {
            "city": "Seattle",
            "date": "not-a-date",
            "temperature_max_c": 22.0,
            "temperature_min_c": 14.0,
            "precipitation_mm": 0.0,
        },
    ])

    return records


def main():
    records = generate_records()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record) + "\n")

    print(f"Created {OUTPUT_PATH.relative_to(PROJECT_ROOT)}")
    print(f"Raw records: {len(records)}")
    print("Expected clean records after transformation: 21")


if __name__ == "__main__":
    main()
