"""Vérifie l'intégrité d'un lot de wheels Fabric.

Deux contrôles indépendants :

1. **Empreintes locales** — chaque wheel correspond à `SHA256SUMS.txt`. Détecte
   un fichier tronqué, corrompu ou manquant après un transfert. Ne demande
   aucun réseau : c'est le contrôle à jouer côté lakehouse, après dépôt.

2. **Empreintes publiées** — chaque wheel correspond au fichier que PyPI sert
   sous ce nom et cette version. Détecte un miroir altéré ou un fichier
   substitué entre PyPI et le lot. À jouer côté poste, avant dépôt.

Ce que ces contrôles ne prouvent pas : que ce que PyPI publie est sain. PyPI ne
signe plus les paquets depuis 2023 ; un mainteneur compromis publierait un
fichier dont l'empreinte serait, elle aussi, « conforme ». Le contrôle des
vulnérabilités connues est un travail distinct — voir audit_dependencies.py.

Usage :
    python scripts/verify_wheels.py                    # fabric-wheels/, les deux contrôles
    python scripts/verify_wheels.py --offline          # empreintes locales seules
    python scripts/verify_wheels.py /chemin/vers/libs  # un autre dossier

Sans dépendance : stdlib seule, pour pouvoir être collé tel quel dans une
cellule de notebook Fabric.
"""

import argparse
import hashlib
import json
import pathlib
import sys
import time
import urllib.request

PYPI_TIMEOUT_SECONDS = 30
PYPI_ATTEMPTS = 3
CHECKSUMS_FILE = "SHA256SUMS.txt"

# PyPI est derrière un CDN qui filtre plus volontiers le User-Agent par défaut
# d'urllib, surtout depuis des plages d'IP partagées comme celles des runners
# CI. Un agent explicite est aussi ce que PyPI demande à ses clients.
USER_AGENT = "flume-lib-verify-wheels (+https://github.com/Y0hannH/flume-lib)"

# Construit ici, jamais publié : aucune empreinte de référence côté PyPI.
LOCAL_PACKAGE = "flume-lib"


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def wheel_identity(filename: str) -> tuple[str, str]:
    """(nom de distribution normalisé, version) d'après le nom du wheel."""
    parts = filename.split("-")
    return parts[0].replace("_", "-").lower(), parts[1]


def check_local_digests(folder: pathlib.Path) -> list[str]:
    """Chaque wheel correspond-il à SHA256SUMS.txt ?"""
    checksums = folder / CHECKSUMS_FILE
    if not checksums.exists():
        return [f"{CHECKSUMS_FILE} absent de {folder}"]

    expected = {}
    for line in checksums.read_text(encoding="utf-8").splitlines():
        if line.strip():
            digest, name = line.split("  ", 1)
            expected[name] = digest

    problems = []
    for name, digest in sorted(expected.items()):
        path = folder / name
        if not path.exists():
            problems.append(f"{name} : listé dans {CHECKSUMS_FILE}, absent du dossier")
        elif sha256(path) != digest:
            problems.append(f"{name} : empreinte différente de {CHECKSUMS_FILE}")
        else:
            print(f"  {name}")

    # un wheel présent mais non listé n'est couvert par aucune empreinte
    for path in sorted(folder.glob("*.whl")):
        if path.name not in expected:
            problems.append(f"{path.name} : présent mais absent de {CHECKSUMS_FILE}")

    print(f"  -> {len(expected) - len(problems)}/{len(expected)} conformes")
    return problems


def published_digests(name: str, version: str) -> dict[str, str]:
    url = f"https://pypi.org/pypi/{name}/{version}/json"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_error: Exception | None = None
    for attempt in range(PYPI_ATTEMPTS):
        try:
            with urllib.request.urlopen(request, timeout=PYPI_TIMEOUT_SECONDS) as response:
                payload = json.load(response)
            return {f["filename"]: f["digests"]["sha256"] for f in payload["urls"]}
        except Exception as exc:  # noqa: BLE001 — réseau, CDN, throttling
            last_error = exc
            if attempt < PYPI_ATTEMPTS - 1:
                time.sleep(2 ** attempt)
    raise last_error


def check_against_pypi(folder: pathlib.Path) -> tuple[list[str], list[str]]:
    """Chaque wheel correspond-il au fichier que PyPI sert sous ce nom ?

    Retourne (divergences, échecs d'interrogation). Les deux empêchent de
    conclure, mais pas pour la même raison : une divergence est un verdict, un
    échec réseau est une absence de verdict.
    """
    problems = []
    unreachable = []
    for path in sorted(folder.glob("*.whl")):
        name, version = wheel_identity(path.name)
        if name == LOCAL_PACKAGE:
            print(f"  {path.name} — construit localement, non publié")
            continue
        try:
            published = published_digests(name, version)
        except Exception as exc:  # noqa: BLE001 — réseau, 404, PyPI indisponible
            unreachable.append(f"{path.name} : interrogation de PyPI impossible ({exc})")
            continue
        if path.name not in published:
            problems.append(f"{path.name} : ce fichier n'existe pas sur PyPI")
        elif published[path.name] != sha256(path):
            problems.append(f"{path.name} : EMPREINTE DIFFÉRENTE de celle publiée")
        else:
            print(f"  {path.name}")
    return problems, unreachable


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "folder", nargs="?", default="fabric-wheels",
        help="Dossier contenant les wheels (défaut : fabric-wheels)",
    )
    parser.add_argument(
        "--offline", action="store_true",
        help="Empreintes locales seules, sans interroger PyPI",
    )
    args = parser.parse_args()

    folder = pathlib.Path(args.folder)
    if not folder.is_dir():
        print(f"Dossier introuvable : {folder}")
        return 2

    wheels = list(folder.glob("*.whl"))
    if not wheels:
        print(f"Aucun wheel dans {folder}")
        return 2

    print(f"{len(wheels)} wheel(s) dans {folder}\n")

    print(f"1. Empreintes locales ({CHECKSUMS_FILE})")
    problems = check_local_digests(folder)

    unreachable: list[str] = []
    if not args.offline:
        print("\n2. Empreintes publiées par PyPI")
        mismatches, unreachable = check_against_pypi(folder)
        problems += mismatches
    else:
        print("\n2. Empreintes publiées par PyPI — ignoré (--offline)")

    if problems:
        print(f"\n{len(problems)} divergence(s) :")
        for problem in problems:
            print(f"  - {problem}")
        if unreachable:
            print(f"  ... et {len(unreachable)} interrogation(s) impossible(s)")
        return 1

    if unreachable:
        print(f"\n{len(unreachable)} interrogation(s) de PyPI impossible(s) :")
        for problem in unreachable:
            print(f"  - {problem}")
        print(
            "\nLes empreintes locales sont conformes, mais la comparaison à PyPI "
            "n'a pas pu être menée — ce n'est pas un contrôle réussi.\n"
            "Relancer, ou --offline pour se limiter au contrôle local."
        )
        return 2

    print("\nLot conforme.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
