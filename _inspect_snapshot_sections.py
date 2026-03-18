import json, pathlib, pprint

p = pathlib.Path("data/snapshots/latest_1130.json")
d = json.loads(p.read_text(encoding="utf-8"))

print("snapshot_file =", p)
print("top_keys =", list(d.keys()))
print()

meta = d.get("meta", {})
print("meta =")
for k in ("mode", "generated_at", "generated_at_jst", "generated_at_utc", "is_true_timepoint", "source_profile", "includes_kabu"):
    print(f"  {k} = {meta.get(k)}")
print()

for k in ("sector_summary", "leaders_by_sector", "focus_candidates", "diagnostics"):
    v = d.get(k)
    if isinstance(v, list):
        print(f"{k}: list, len={len(v)}")
        for i, row in enumerate(v[:5], 1):
            print(f"  [{i}] {row}")
    elif isinstance(v, dict):
        print(f"{k}: dict, len={len(v)}")
        items = list(v.items())[:5]
        for i, (kk, vv) in enumerate(items, 1):
            print(f"  [{i}] key={kk}")
            print(f"      value={vv}")
    else:
        print(f"{k}: {type(v).__name__} -> {v}")
    print()
