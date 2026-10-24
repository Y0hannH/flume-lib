# Development

Working on flume-lib itself: running the checks, what CI does, and how a release is cut. To *use* the library, start from the [README](../README.md).

## Setup and checks

```bash
git clone https://github.com/Y0hannH/flume-lib.git
cd flume-lib
pip install -e ".[dev]"
ruff check .
mypy
bandit -r src -ll -q
pytest --cov
python scripts/gen_key_index.py --check
```

Unit tests are fully mocked — no network calls. Python ≥ 3.10 is required locally. `ruff` also caps cyclomatic complexity at 15 per function, `mypy` checks the annotations the wheel publishes (`py.typed`), `pytest --cov` fails under 95 % coverage (the threshold is in [`pyproject.toml`](../pyproject.toml)), and `gen_key_index.py --check` fails when a generated key reference has drifted from the code.

Run these exact commands before pushing: CI runs all of them, and one skipped locally is one that fails on the pull request.

Use `pytest`, not `python -m pytest`: the latter puts the current directory on `sys.path` and hides import errors that CI would catch.

## What CI does

[`ci.yml`](../.github/workflows/ci.yml) runs on every push and pull request, and every Monday:

| Job | Checks |
|---|---|
| `tests` (from [`tests.yml`](../.github/workflows/tests.yml)) | Lint, types, security lint and the suite on Python 3.10, 3.11 and 3.12 — the three Fabric kernel versions; the lowest allowed version of every dependency (`minimum-versions`); the suite on Windows. |
| `readme-shas` | Every published tag has its SHA in the table of [install.md](install.md#alternative-install-from-github-development-environments). |
| `key-index` | The generated key references match the code. |
| `lockfile` | `requirements/runtime.txt` and `requirements/build.txt` match their sources. |
| `audit` | [OSV.dev](https://osv.dev) lists no known vulnerability in the runtime dependencies. |
| `build` | The wheel builds and carries every module and the `py.typed` marker. |

The weekly run is the point of `audit`: an offline install freezes its versions, so a CVE published after a release would surface nowhere otherwise. See [Verifying a batch](security.md#verifying-a-batch) for what the two verification scripts do — and do not — prove.

Dependabot proposes updates to the GitHub Actions (pinned by commit SHA), to the bounds in `pyproject.toml` and to the lockfiles, weekly.

## Two generated sections

[`docs/cookbook.md`](cookbook.md) and [`docs/agent-cookbook.md`](agent-cookbook.md) each carry a key reference between `BEGIN GENERATED KEY INDEX` and `END GENERATED KEY INDEX`. It is written by [`scripts/gen_key_index.py`](../scripts/gen_key_index.py) from the validator and from the defaults read in the implementation. Do not edit it by hand: after adding or changing a config key, run `python scripts/gen_key_index.py` and commit the result.

`tests/test_docs.py` also checks the documentation: every complete config in the agent cookbook validates, every Python block parses and imports only names the package exports, every relative link and anchor in the README and `docs/` resolves, and the pinned `flume-lib==X.Y.Z` of the install lines equals the package version.

## Release procedure

1. Bump the version in `pyproject.toml` and `src/flume_lib/__init__.py`, **and the pinned version of the `%pip install` lines** in the README, in `docs/` and in `examples/` (`grep -rn "flume-lib==" README.md docs examples`; `tests/test_docs.py` fails when one is missed). These belong to the release commit, not after the tag: otherwise the tagged tree — and every artifact built from it — tells readers to install the previous version.
2. If the dependencies should move, refresh the lockfile — `uv pip compile pyproject.toml --universal --python-version 3.10 --generate-hashes --no-header --upgrade -o requirements/runtime.txt` — and review the diff: it is the list of what will enter the offline bundle. The build tools have their own (`requirements/build.in` to `requirements/build.txt`, same command). CI fails when a lockfile and its source disagree.
3. Add the version's section to [CHANGELOG.md](../CHANGELOG.md), breaking changes first. The release workflow uses it as the release notes and refuses to publish without it.
4. `ruff check . && mypy && bandit -r src -ll -q && pytest --cov`
5. Commit the release, then tag that commit: `git tag -a vX.Y.Z`.
6. Add the tagged SHA to the table of [install.md](install.md#alternative-install-from-github-development-environments) (`git rev-list -n1 vX.Y.Z`) in a **separate** commit — a commit cannot contain its own SHA, so this one always lands after the tag. `python scripts/check_readme_shas.py` prints the line to add, and CI fails on the next push if it is missing: v0.10.1 and v0.10.2 were both forgotten here before the check existed.
7. Push the branch and the tag. Tags are protected against update and deletion: review the tag before pushing, it cannot be moved afterwards.
8. Pushing the tag triggers [the release workflow](../.github/workflows/release.yml): it builds one offline batch **per Fabric kernel version** (3.10, 3.11, 3.12), verifies each one (digests against PyPI, known vulnerabilities), attests the files with build provenance, and publishes the GitHub Release with the three zips and the wheel attached, using the CHANGELOG section as its notes. Nothing to build by hand.

   A batch must be built by the interpreter of the version it targets — `pip download --python-version` only drives wheel tags and `Requires-Python`, never environment markers — which is why there is one runner per version rather than one runner for three batches. Building a 3.11 batch from a 3.12 machine silently drops `typing-extensions`, and the offline install then fails at the client. `build_fabric_wheels.py` refuses that combination by default.

   To publish a tag that is already pushed, run the workflow manually from the Actions tab with the tag as input.
9. Download the batch matching the kernel from the Release, upload the `.whl` files to `Files/libs/`, and verify them on the lakehouse side with `scripts/verify_wheels.py` ([install.md](install.md#recommended-offline-wheels-from-the-lakehouse)).

The attestation step only runs on a tag, so a change to the release workflow cannot be exercised by a pull request: cut a pre-release tag to test it.
