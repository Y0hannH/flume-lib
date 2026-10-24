# Fabric Python notebook (non-Spark) — example consumer of flume-lib.
# Prerequisites: schema-enabled lakehouse; wheels uploaded to the lakehouse
# (see scripts/build_fabric_wheels.py); each source in the JSON declares
# target_schema + target_table.

# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.20.0

import json

from flume_lib import run_sources

with open("/lakehouse/default/Files/conf/sources.json") as f:
    sources = json.load(f)

outcome = run_sources(sources)   # one after the other; a failing source stops no other
print(outcome)                   # a total, then one line per source
outcome.raise_for_failures()     # fails the notebook, which is what a pipeline sees
