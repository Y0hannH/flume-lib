# Fabric Python notebook (non-Spark) — Microsoft Graph and OData v4 endpoints.
#
# The concrete case of a "standard paginated API": OData is a specification,
# so every endpoint that speaks it — Graph, Dynamics 365, Business Central,
# SAP's OData services — paginates the same way, and the same three options
# cover them all: `next_link` on `@odata.nextLink`, records under `value`, and
# `params` for the `$`-prefixed query options.
#
# Auth is the other half: an Entra ID service principal, which is
# `oauth2_client_credentials` with `tenant_id` instead of a token URL.
#
# %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.9.0

from datetime import date, datetime, timedelta, timezone

from flume_lib import run_source

TENANT_ID = "00000000-0000-0000-0000-000000000000"
KEYVAULT = "https://mykv.vault.azure.net"
GRAPH = "https://graph.microsoft.com/v1.0"

# App registration with application permissions (User.Read.All, Directory.Read.All
# …), admin-consented. `tenant_id` builds the Entra token URL; `.default` asks
# for exactly the application permissions already granted to the app — a scope
# list here would be the delegated flow, which a notebook has no user for.
SERVICE_PRINCIPAL = {
    "type": "oauth2_client_credentials",
    "tenant_id": TENANT_ID,
    "client_id": {"keyvault_url": KEYVAULT, "secret_name": "graph-sp-client-id"},
    "client_secret": {"keyvault_url": KEYVAULT, "secret_name": "graph-sp-client-secret"},
    "scope": "https://graph.microsoft.com/.default",
}

BASE = {
    "auth": SERVICE_PRINCIPAL,
    "target_schema": "bronze",
    # Graph throttles per tenant and per resource, and says so with a 429 plus a
    # `Retry-After`. The library waits exactly what it is told rather than its
    # own backoff — retrying earlier than a Microsoft endpoint asked is how a
    # tenant ends up throttled harder.
    "retry": {"max_attempts": 5, "backoff_multiplier": 2},
}

# `next_link` is the whole of OData pagination: the response carries
# `@odata.nextLink`, a complete URL with the `$skiptoken` already in it, and the
# library follows it until the field is gone. Note that `@odata.nextLink` is a
# top-level key that happens to contain a dot — not a dotted path.
#
# The `params` below are sent on the **first call only**, which is exactly
# right: `@odata.nextLink` already carries `$select` and `$filter` forward, and
# re-appending them would duplicate the query options.
ODATA_PAGINATION = {
    "type": "next_link",
    "next_field": "@odata.nextLink",
    "items_field": "value",
}


# ---------------------------------------------------------------------------
# Full reload of a bounded entity.
#
# `$select` is not an optimization here, it is the contract: without it Graph
# returns a default property set that it is allowed to change, and every added
# property lands in Delta as a new column (writes use schema_mode=merge). Name
# the columns you want and the table shape stops depending on Microsoft.
#
# `$top` is a page size, not a total — 999 is the maximum for directory
# objects. Graph may return fewer; `next_link` does not care, it follows the
# link until there is none.
# ---------------------------------------------------------------------------

USERS = {
    **BASE,
    "name": "graph_users",
    "base_url": f"{GRAPH}/users",
    "target_table": "users",
    "params": {
        "$select": "id,displayName,mail,userPrincipalName,jobTitle,department,accountEnabled",
        "$top": 999,
    },
    "pagination": ODATA_PAGINATION,
}

GROUPS = {
    **BASE,
    "name": "graph_groups",
    "base_url": f"{GRAPH}/groups",
    "target_table": "groups",
    "params": {
        "$select": "id,displayName,mail,groupTypes,createdDateTime",
        "$top": 999,
    },
    "pagination": ODATA_PAGINATION,
}

# Advanced query options — `$count`, `$search`, `$filter` on properties Graph
# calls "advanced" — require the `ConsistencyLevel: eventual` header *and*
# `$count=true`, together. One without the other is a 400 that names neither.
# `headers` takes literal strings only; a credential belongs in `auth`.
GUEST_USERS = {
    **USERS,
    "name": "graph_guest_users",
    "target_table": "guest_users",
    "headers": {"ConsistencyLevel": "eventual"},
    "params": {
        **USERS["params"],
        "$filter": "userType eq 'Guest'",
        "$count": "true",
    },
}


# ---------------------------------------------------------------------------
# Why there is no `incremental` block above.
#
# `inject: "query_param"` sends the watermark as the entire value of one query
# param. It fits `?updated_since=2026-08-01T00:00:00Z`; it cannot build
# `?$filter=lastModifiedDateTime ge 2026-08-01T00:00:00Z`, because OData wants
# the value *inside* an expression. Composing one is `inject: "body_template"`,
# which substitutes into `body` and therefore needs a POST — and these
# endpoints are GET.
#
# So, for an OData source, one of:
#
#   - full reload, when the entity is small enough (the two above: a directory
#     of a few thousand objects costs a handful of calls);
#   - a bounded window computed in the notebook and written straight into
#     `$filter`, below — the watermark lives in the code rather than in
#     `flume.watermark`;
#   - Graph delta queries (`/users/delta`), which hand back a `@odata.deltaLink`
#     to replay on the next run. That link must be persisted between runs, which
#     `flume.watermark` — a max over a record field — does not do. Out of scope
#     for now.
#
# The windowed form re-reads its overlap on every run: rows are appended, not
# merged, so the same record can land twice. Deduplicate downstream on the
# business key, keeping the highest `_flume_ingested_at` — the lineage columns
# are on every written row for exactly this.
# ---------------------------------------------------------------------------


def odata_timestamp(when: datetime) -> str:
    """OData v4 wants an unquoted ISO 8601 UTC literal: 2026-08-01T00:00:00Z."""
    return when.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def signins_window(days: int = 1):
    """Sign-in logs over a sliding window. A day of overlap is deliberate: a
    scheduled run that fails on Sunday is covered by Monday's."""
    since = odata_timestamp(datetime.now(timezone.utc) - timedelta(days=days))
    return {
        **BASE,
        "name": "graph_signins",
        "base_url": f"{GRAPH}/auditLogs/signIns",
        "target_table": "signins",
        "params": {
            "$filter": f"createdDateTime ge {since}",
            "$top": 1000,
        },
        "pagination": ODATA_PAGINATION,
    }


# ---------------------------------------------------------------------------
# The same three options against another OData service.
#
# Business Central here — the point being that nothing above was Graph-specific.
# Only the base URL, the auth scope and the entity names change; the pagination
# block is copied verbatim.
# ---------------------------------------------------------------------------

BC_COMPANY_ID = "00000000-0000-0000-0000-000000000000"
BC_BASE_URL = (
    f"https://api.businesscentral.dynamics.com/v2.0/{TENANT_ID}/Production/api/v2.0"
    f"/companies({BC_COMPANY_ID})"
)

BUSINESS_CENTRAL_SP = {
    **SERVICE_PRINCIPAL,
    "scope": "https://api.businesscentral.dynamics.com/.default",
}


def business_central_source(entity: str, table: str, filter_expression: str = ""):
    config = {
        **BASE,
        "auth": BUSINESS_CENTRAL_SP,
        "name": f"bc_{table}",
        "base_url": f"{BC_BASE_URL}/{entity}",
        "target_table": table,
        "pagination": ODATA_PAGINATION,
    }
    if filter_expression:
        config["params"] = {"$filter": filter_expression}
    return config


def business_central_sources(since: date):
    bound = since.isoformat()
    return [
        business_central_source("customers", "bc_customers"),
        business_central_source("items", "bc_items"),
        business_central_source(
            "salesInvoices", "bc_sales_invoices", f"postingDate ge {bound}"
        ),
    ]


# ---------------------------------------------------------------------------
# Running them.
# ---------------------------------------------------------------------------


def run_all():
    sources = [USERS, GROUPS, GUEST_USERS, signins_window()]
    sources += business_central_sources(date(date.today().year, 1, 1))
    for config in sources:
        result = run_source(config)
        print(f"{result.source_name}: {result.status} ({result.rows_loaded} rows)")
        if result.status == "failed":
            print(f"  {result.error_message}")


# Nested Graph properties (`assignedLicenses`, `businessPhones`, `manager`) land
# in Delta as JSON strings, one column each, parsed downstream.
#
# Check the app registration, the admin consent and the response shape without
# writing anything — a missing application permission answers 403 here, not
# further down:
#   run_source(USERS, dry_run=True)
#
# run_all()
