import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

GROUPS_URL = "https://production.api.aws.ccg-ops.com/api/coins/research/groups/"
POP_URL = "https://production.api.aws.ccg-ops.com/api/coins/research/population"


def fetch_json(url):
    r = requests.get(url)
    r.raise_for_status()
    return r.json()


def get_all_group_ids(subcategory_id=187):
    """Fetch all researchGroupIDs from /groups/ (paged)."""
    page, ids = 1, []
    while True:
        url = f"{GROUPS_URL}?researchSubcategoryID={subcategory_id}&page={page}&keywords=&languageID="
        data = fetch_json(url)
        items = data.get("Items", [])
        if not items:
            break
        ids.extend([item["researchGroupID"] for item in items])
        if not data.get("ShowNextPage"):
            break
        page += 1
    return ids


def map_grade(number: int, designation: str) -> str:
    """Map numeric grade to proper prefix (MS, AU, XF, etc)."""
    if 60 <= number <= 70:
        return f"MS{number}"
    elif 50 <= number <= 59:
        return f"AU{number}"
    elif 40 <= number <= 49:
        return f"XF{number}"
    elif 20 <= number <= 39:
        return f"VF{number}"
    elif 12 <= number <= 19:
        return f"F{number}"
    elif 6 <= number <= 11:
        return f"G{number}"
    elif 3 <= number <= 5:
        return f"AG{number}"
    elif 1 <= number <= 2:
        return f"PO{number}"
    else:
        # fallback: keep whatever designation (PF, MS) with the number
        return f"{designation}{number}"


def get_grades_for_group(group_id, designation="PF"):
    """Fetch all pages for a group and return ALL grades with counts for each coin."""
    results, page = [], 1
    while True:
        url = f"{POP_URL}/{designation}/?researchGroupID={group_id}&page={page}&keywords=&populationID="
        data = fetch_json(url)
        items = data.get("Items", [])
        if not items:
            break

        for coin in items:
            display_name = coin.get("displayName", f"Group {group_id}")
            grade_counts = []
            for key, value in coin.items():
                if key.startswith("population_") and isinstance(value, int) and value > 0:
                    try:
                        grade = int(key.split("_")[1])  # e.g. population_69 → 69
                        grade_label = map_grade(grade, designation)
                        grade_counts.append((grade_label, value))
                    except ValueError:
                        continue
            if grade_counts:
                # Sort by grade number (descending)
                grade_counts.sort(key=lambda x: int("".join([c for c in x[0] if c.isdigit()])), reverse=True)
                results.append({
                    "GroupID": group_id,
                    "Coin Name": display_name,
                    "Designation": designation,
                    "Grades": [{"Grade": g, "Count": c} for g, c in grade_counts]
                })

        if not data.get("ShowNextPage"):
            break
        page += 1
    return results


def main():
    group_ids = get_all_group_ids(subcategory_id=187)
    print(f"Found {len(group_ids)} group IDs")

    all_rows = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {}
        for gid in group_ids:
            futures[executor.submit(get_grades_for_group, gid, "PF")] = (gid, "PF")
            futures[executor.submit(get_grades_for_group, gid, "MS")] = (gid, "MS")

        for i, future in enumerate(as_completed(futures), start=1):
            gid, des = futures[future]
            try:
                coin_results = future.result()
                print(f"[{i}/{len(futures)}] Done group {gid} ({des}, {len(coin_results)} coins)")
                all_rows.extend(coin_results)
            except Exception as e:
                print(f"❌ Error fetching {gid} ({des}): {e}")

    # Write JSON (this file will be committed to the repo by GitHub Actions)
    with open("ngc_population.json", "w", encoding="utf-8") as f:
        json.dump(all_rows, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved ngc_population.json with {len(all_rows)} rows")


if __name__ == "__main__":
    main()
