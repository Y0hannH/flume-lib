# flume-lib

Accélérateur d'ingestion API générique pour notebooks **Microsoft Fabric Python** (non-Spark). Python pur : écriture Delta via [delta-rs](https://github.com/delta-io/delta-rs) (`deltalake`), sans dépendance à PySpark.

## Installation

```
%pip install git+https://github.com/Y0hannH/flume-lib.git@v0.1.0
```

## Usage

```python
from flume_lib import run_source
import json

with open("/lakehouse/default/Files/conf/sources.json") as f:
    sources = json.load(f)

for source_config in sources:
    result = run_source(source_config)
    print(f"{source_config['name']}: {result.status} ({result.rows_loaded} lignes)")
```

`run_source(config, lakehouse_tables_path="/lakehouse/default/Tables")` ne lève **jamais** d'exception : toute erreur est catchée et remontée dans le `RunResult` (`status`, `rows_loaded`, `error_message`, `start_ts`, `end_ts`, `run_id`), pour que la boucle appelante continue sur la source suivante.

## Configuration d'une source

```json
{
  "name": "source_exemple",
  "base_url": "https://api.exemple.com/v1/items",
  "auth": {
    "type": "bearer_token",
    "token_env_var": "SOURCE_EXEMPLE_TOKEN"
  },
  "pagination": {
    "type": "offset",
    "limit": 100,
    "limit_param": "limit",
    "offset_param": "offset"
  },
  "incremental": {
    "enabled": true,
    "field": "updated_at",
    "param_name": "updated_since"
  },
  "target_table": "bronze_source_exemple",
  "retry": {
    "max_attempts": 3,
    "backoff_multiplier": 1
  }
}
```

### Auth

Les credentials ne sont **jamais** en clair dans la config — uniquement des noms de variables d'environnement, résolues au runtime (à injecter depuis Key Vault avant l'appel).

| Type | Clés de config | Statut |
|---|---|---|
| `bearer_token` | `token_env_var` | ✅ |
| `api_key_header` | `key_env_var`, `header_name` (défaut `X-API-Key`) | ✅ |
| `basic` | `username_env_var`, `password_env_var` | ✅ |
| `oauth2_client_credentials` | — | stub |

### Pagination

| Type | Clés de config | Statut |
|---|---|---|
| `offset` | `limit`, `limit_param`, `offset_param` | ✅ |
| `next_link` | `next_field` (défaut `next`), `items_field` | ✅ |
| `cursor` | — | stub |

`items_field` (optionnel, toutes stratégies) : nom du champ de la réponse contenant les enregistrements. À défaut, la lib détecte une réponse liste, ou cherche `data` / `items` / `results` / `value`.

### Incrémental (watermark)

Si `incremental.enabled`, le dernier watermark est lu dans la table `watermark` et passé en query param (`param_name`). En fin de run **réussi uniquement**, le max de `incremental.field` sur les enregistrements chargés devient le nouveau watermark. Pas d'avancement du watermark en cas d'échec.

### Retry

Backoff exponentiel via `tenacity` sur les erreurs réseau et HTTP 429/5xx, paramétré par `retry.max_attempts` (défaut 3) et `retry.backoff_multiplier` (défaut 1). Les 4xx (hors 429) échouent immédiatement.

## Tables techniques

Tables Delta créées automatiquement dans le lakehouse (`lakehouse_tables_path`) :

- **`watermark`** : `source_name`, `last_value`, `updated_ts`
- **`log_runs`** : `run_id`, `source_name`, `start_ts`, `end_ts`, `status`, `rows_loaded`, `error_message` — une ligne par appel à `run_source`, succès ou échec

## Développement

```
pip install -e ".[dev]"
pytest
```

Tests unitaires mockés, aucun appel réseau réel.

## Hors scope (v0.1)

- CLI d'installation/scaffolding côté client
- Intégration Key Vault (VaultPulse) — les tokens sont supposés déjà en variables d'environnement
- Auth OAuth2 client_credentials et pagination cursor (stubs)
