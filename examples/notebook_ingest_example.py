# Fabric Python notebook (non-Spark) — example consumer of flume-lib.
# Prerequisites: schema-enabled lakehouse; wheels uploaded to the lakehouse
# (see scripts/build_fabric_wheels.py); each source in the JSON declares
# target_schema + target_table.

# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.14.0

import json

from flume_lib import run_source

with open("/lakehouse/default/Files/conf/sources.json") as f:
    sources = json.load(f)

results = []
for source_config in sources:
    result = run_source(source_config)
    results.append(result)
    print(f"{source_config['name']}: {result.status} ({result.rows_loaded} rows)")

failed = [r for r in results if r.status == "failed"]
if failed:
    print(f"\n{len(failed)} source(s) failed:")
    for r in failed:
        print(f"  - {r.source_name}: {r.error_message}")
