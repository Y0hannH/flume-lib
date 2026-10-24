# Installation

How to get flume-lib into a Microsoft Fabric Python notebook, and what the notebook environment must provide. For what the library is and whether it fits your case, start from the [README](../README.md).

## Requirements

| What | Detail |
|---|---|
| Notebook | A Microsoft Fabric **Python notebook** (non-Spark). Kernels **3.10, 3.11 and 3.12** are supported and tested in CI; the library needs Python ≥ 3.10. |
| Lakehouse | A **schema-enabled lakehouse** attached as the notebook's default. Schemas cannot be switched on for an existing lakehouse: it must be created with the option on. |
| Key Vault (optional) | To read secrets from Azure Key Vault, the notebook identity needs **Get** permission on the vault's secrets. |
| Dependencies | `requests`, `tenacity`, `deltalake`, `arro3-core`. No PySpark, no pandas, no pyarrow. Version bounds are in [`pyproject.toml`](../pyproject.toml); the exact versions that ship in an offline batch are in [`requirements/runtime.txt`](../requirements/runtime.txt). |
| Extra `[azure]` | `azure-identity` and `azure-keyvault-secrets`, only to resolve Key Vault references **outside** Fabric. In Fabric, `notebookutils` is preinstalled and is used instead. |

The library is not published on PyPI. It is installed from wheels you upload to the lakehouse (recommended) or from a pinned GitHub commit.

## Recommended: offline wheels from the lakehouse

No code is fetched from GitHub or PyPI when the notebook runs: it installs exactly the files you uploaded.

1. **Get the batch for your kernel.** Each [GitHub Release](https://github.com/Y0hannH/flume-lib/releases) carries one zip per kernel version (3.10, 3.11, 3.12) holding the library, all its dependencies and a `SHA256SUMS.txt`. Or build it yourself on a workstation (library + dependencies resolved for the Fabric kernel — Linux x86_64 — and Python 3.12 by default):

   ```bash
   git clone https://github.com/Y0hannH/flume-lib.git
   cd flume-lib
   pip install -e ".[dev]"
   python scripts/build_fabric_wheels.py          # add --python-version 3.11 for another kernel
   ```

   The build downloads the dependencies from the hashed lockfile `requirements/runtime.txt` (`--require-hashes`): a file that differs from the one that was reviewed stops the build.

2. **Upload the `.whl` files** to a folder of the lakehouse — `Files/libs/` by convention. The layout is up to you: any path works as long as the same path is given to `--find-links`.

3. **In the notebook:**

   ```python
   %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==0.20.0
   ```

`--no-index` guarantees pip resolves only from that folder.

**Pin the version with `==`.** Without a pin, pip takes the highest version present in the folder, so dropping a newer batch next to the old one silently upgrades every notebook that points at it. With the pin, upgrading is a deliberate act: upload the new batch and change the pin in the notebooks you want to move. Keep the previous batch archived (elsewhere in `Files/`, or in the release zip) so that a rollback is re-uploading it.

**Verify the upload** from the lakehouse side — the transfer is where a file gets truncated:

```bash
python scripts/verify_wheels.py /lakehouse/default/Files/libs --offline
```

`--offline` skips the comparison with PyPI, so it needs no network. The script is standard-library only and can be pasted into a notebook cell when no shell is available. What this and the vulnerability audit do and do not prove: [security.md](security.md#verifying-a-batch).

Releases are signed: a downloaded file can be checked with `gh attestation verify <file> --repo Y0hannH/flume-lib` (see [security.md](security.md#supply-chain)).

## Alternative: install from GitHub (development environments)

Pin the **full commit SHA** of a release, never a tag: a tag can be moved by someone with write access to the repository, a SHA cannot.

```
%pip install git+https://github.com/Y0hannH/flume-lib.git@<full-commit-sha-from-the-table-below>
```

| Version | SHA |
|---|---|
| v0.19.0 | `bf1493bb874fdc069e6dced5208517073b92f70c` |
| v0.20.0 | `6f3afd8073e4d7c73e30c3ecbe8df6435d297138` |
| v0.18.0 | `e1f0988887c3b6e512cd45724a399145320c5c95` |
| v0.17.0 | `5e5caaaf82922f0813d2da82bfbdff25e137a12b` |
| v0.16.0 | `dad44901a257e3a3b07ca303fd00e11060c5b899` |
| v0.15.0 | `0087a656f1c1e073e3aa359f547dfc771bd6b876` |
| v0.14.0 | `adab6dced6d596c63612ac8340560274fd985872` |
| v0.13.0 | `cc9194d4de86fb9dc3897e9abfa2f67adadeab1e` |
| v0.12.0 | `08fa0ee46487ee08cbe791bfd045f1971dbf8b76` |
| v0.11.1 | `317604e68657fd5cabfe1740863924d7d35583af` |
| v0.11.0 | `411dfcc762f8ffe3744c1ba494ddf30585cdb9a8` |
| v0.10.2 | `3e9c199bbe0def4c279809c166129f90ac8e01c8` |
| v0.10.1 | `aaf83b668cd4c3840cb8e8f394340d0e596fb3a3` |
| v0.10.0 | `67c0d77537ed80a73effda8a9bc26e766174c805` |
| v0.9.0 | `809a82df70d6790574853e1329b8f3bf5b1b663c` |
| v0.8.1 | `8e26f5e250a690f77b7ca7e196c855588efd76eb` |
| v0.8.0 | `526840b400baaf5de3889af5b0d783833fa9066a` |
| v0.7.0 | `d69aad0b1539558266fdbf02f20b327bbf4f3c71` |
| v0.6.0 | `c71716012f6c846d900ddae10892c683ec5b58b3` |
| v0.5.0 | `679a15adf13b7169a520194b76ee8cfd2cfe48aa` |
| v0.4.0 | `75fb767dad8c453930ad5249c2b4540c5f263ce0` |

Find a version's SHA with `git rev-list -n1 vX.Y.Z` or on the [tags page](https://github.com/Y0hannH/flume-lib/tags).

This table is kept honest by CI: [`scripts/check_readme_shas.py`](../scripts/check_readme_shas.py) fails the build when a published tag is missing from it. A tag can only be added to the table by a commit that comes *after* it — a commit cannot contain its own SHA — so the check ignores the tag being published and starts asking on the next commit. (The script keeps its historical name: the table used to live in the README.)

## Delta writes in Fabric (OneLake)

The local `/lakehouse/default/...` mount of a Fabric notebook does not support the atomic rename that the delta-rs transaction-log commit needs (`Operation not permitted (os error 1)`, table left without a valid `_delta_log`). The library works around it automatically: in Fabric, the default path is resolved to the OneLake **ABFSS** URI of the notebook's default lakehouse, and writes authenticate with a storage token obtained through `notebookutils`. Nothing to configure.

The default lakehouse may live in another workspace than the notebook: the URI carries the lakehouse's workspace, not the notebook's. If the runtime context does not expose it, resolution falls back to the notebook's workspace and the run fails with a 404 on `_delta_log`; pass `lakehouse_tables_path` explicitly in that case. A path that is still `/lakehouse/default/...` after resolution makes the run add a warning to `RunResult.warnings`.

To target another lakehouse, pass its ABFSS URI:

```python
run_source(config, lakehouse_tables_path="abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables")
```

Outside Fabric (Azure storage or a local folder), `storage_options` is passed to delta-rs as is.
