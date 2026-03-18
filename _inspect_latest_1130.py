import json, pathlib

p = pathlib.Path("data/snapshots/latest_1130.json")
d = json.loads(p.read_text(encoding="utf-8"))

rows = None
row_key = None
if isinstance(d, dict):
    for k in ("rows", "records", "data", "items"):
        v = d.get(k)
        if isinstance(v, list):
            rows = v
            row_key = k
            break

print("snapshot_file =", p)
print("top_level_type =", type(d).__name__)
if isinstance(d, dict):
    print("top_keys =", list(d.keys())[:20])

if rows is None:
    print("rows_not_found = True")
else:
    print("rows_key =", row_key)
    print("row_count =", len(rows))
    print("first_rows =")
    for i, r in enumerate(rows[:5], 1):
        if isinstance(r, dict):
            print(i, {
                "code": r.get("code") or r.get("Code") or r.get("証券コード"),
                "name": r.get("name") or r.get("銘柄") or r.get("銘柄名"),
                "sector": r.get("S33Nm") or r.get("セクター") or r.get("業種"),
            })
        else:
            print(i, r)
