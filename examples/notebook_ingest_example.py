# Notebook Fabric Python (non-Spark) — exemple de consommation de flume-lib.
# Prérequis : les tokens sont déjà injectés en variables d'environnement
# (depuis Key Vault) avant l'exécution de cette boucle.

# %pip install git+https://github.com/<org>/flume-lib.git@v0.1.0

import json

from flume_lib import run_source

with open("/lakehouse/default/Files/conf/sources.json") as f:
    sources = json.load(f)

results = []
for source_config in sources:
    result = run_source(source_config)
    results.append(result)
    print(f"{source_config['name']}: {result.status} ({result.rows_loaded} lignes)")

failed = [r for r in results if r.status == "failed"]
if failed:
    print(f"\n{len(failed)} source(s) en échec :")
    for r in failed:
        print(f"  - {r.source_name}: {r.error_message}")
