import json
for p in ["docs/data/states_latest.json","docs/data/counties_latest.json"]:
    x=json.load(open(p,encoding="utf-8"))
    txt=json.dumps(x)
    print(p, "Alaska:", "Alaska" in txt or '"02"' in txt, "Hawaii:", "Hawaii" in txt or '"15"' in txt)
