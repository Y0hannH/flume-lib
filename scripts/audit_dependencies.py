"""Interroge OSV.dev pour les vulnérabilités connues des dépendances.

Deux modes, deux questions différentes :

- **Sans argument** — audite la fermeture des dépendances d'exécution telle
  qu'elle est installée. C'est ce que joue la CI : « en installant flume-lib
  aujourd'hui, hérite-t-on d'une vulnérabilité connue ? »

- **`--wheels <dossier>`** — audite un lot de wheels précis, d'après les noms
  de fichiers. C'est ce qu'on joue avant de livrer un lot, et à intervalles
  réguliers ensuite : l'installation hors ligne fige des versions, donc une
  CVE publiée après la release ne se signale nulle part toute seule.

Sort en 1 si une vulnérabilité est connue, 2 si le contrôle n'a pas pu être
mené (réseau, OSV indisponible) — un contrôle qui n'a pas tourné n'est pas un
contrôle réussi.

Ce que cet audit ne couvre pas : les vulnérabilités non publiées, et une
compromission en amont dont le paquet piégé serait, lui, parfaitement conforme
à ce que PyPI publie. Le contrôle d'intégrité est un travail distinct — voir
verify_wheels.py.

Usage :
    python scripts/audit_dependencies.py
    python scripts/audit_dependencies.py --wheels fabric-wheels

Sans dépendance : stdlib seule.
"""

import argparse
import importlib.metadata as metadata
import json
import pathlib
import re
import sys
import urllib.error
import urllib.request

OSV_BATCH_URL = "https://api.osv.dev/v1/querybatch"
OSV_VULN_URL = "https://api.osv.dev/v1/vulns/{id}"
OSV_TIMEOUT_SECONDS = 60

ROOT_PACKAGE = "flume-lib"
# Découpe un « deltalake>=1.0 » ou « typing-extensions ; python_version < '3.12' »
_REQUIREMENT_NAME = re.compile(r"^\s*([A-Za-z0-9._-]+)")


def requirement_name(requirement: str) -> str | None:
    head = requirement.split(";", 1)[0]
    match = _REQUIREMENT_NAME.match(head)
    return match.group(1).lower().replace("_", "-") if match else None


def installed_closure(root: str = ROOT_PACKAGE) -> list[tuple[str, str]]:
    """Dépendances d'exécution effectivement installées, transitives comprises.

    On ne cherche pas à évaluer les marqueurs d'environnement : un paquet exclu
    par son marqueur n'est pas installé, il disparaît donc naturellement. Les
    extras (dev, azure) sont écartés — ils ne partent pas chez le client.
    """
    seen: dict[str, str] = {}
    queue = [root]
    while queue:
        name = queue.pop()
        if name in seen:
            continue
        try:
            distribution = metadata.distribution(name)
        except metadata.PackageNotFoundError:
            continue  # exclu par un marqueur, ou extra non installé
        seen[name] = distribution.version
        for requirement in distribution.requires or []:
            if "extra ==" in requirement:
                continue
            dependency = requirement_name(requirement)
            if dependency:
                queue.append(dependency)
    return sorted(seen.items())


def wheel_closure(folder: pathlib.Path) -> list[tuple[str, str]]:
    packages = {}
    for wheel in folder.glob("*.whl"):
        parts = wheel.name.split("-")
        packages[parts[0].replace("_", "-").lower()] = parts[1]
    return sorted(packages.items())


def query_osv(packages: list[tuple[str, str]]) -> list[list[str]]:
    """Identifiants de vulnérabilité par paquet, dans l'ordre reçu."""
    body = json.dumps({
        "queries": [
            {"package": {"name": name, "ecosystem": "PyPI"}, "version": version}
            for name, version in packages
        ]
    }).encode()
    request = urllib.request.Request(
        OSV_BATCH_URL, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=OSV_TIMEOUT_SECONDS) as response:
        results = json.load(response)["results"]
    return [[v["id"] for v in result.get("vulns", [])] for result in results]


def describe(vulnerability_id: str) -> str:
    try:
        url = OSV_VULN_URL.format(id=vulnerability_id)
        with urllib.request.urlopen(url, timeout=OSV_TIMEOUT_SECONDS) as response:
            payload = json.load(response)
    except Exception:  # noqa: BLE001 — le détail est un confort, pas le verdict
        return ""
    return (payload.get("summary") or payload.get("details", "")).split("\n")[0][:120]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--wheels", metavar="DOSSIER",
        help="Auditer un lot de wheels au lieu de l'environnement installé",
    )
    args = parser.parse_args()

    if args.wheels:
        folder = pathlib.Path(args.wheels)
        if not folder.is_dir():
            print(f"Dossier introuvable : {folder}")
            return 2
        packages = wheel_closure(folder)
        source = f"lot de wheels {folder}"
    else:
        packages = installed_closure()
        source = "environnement installé"

    if not packages:
        print(f"Aucun paquet trouvé ({source})")
        return 2

    print(f"{len(packages)} paquet(s) — {source}\n")
    try:
        vulnerabilities = query_osv(packages)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"OSV injoignable : {exc}")
        print("Contrôle non mené — ce n'est pas un contrôle réussi.")
        return 2

    affected = 0
    # strict : un désalignement attribuerait le verdict d'un paquet à un autre
    for (name, version), identifiers in zip(packages, vulnerabilities, strict=True):
        if identifiers:
            affected += 1
            print(f"  {name} {version} — {len(identifiers)} vulnérabilité(s)")
            for identifier in identifiers:
                summary = describe(identifier)
                print(f"      {identifier}{' — ' + summary if summary else ''}")
                print(f"      https://osv.dev/vulnerability/{identifier}")
        else:
            print(f"  {name} {version}")

    if affected:
        print(f"\n{affected} paquet(s) porteur(s) d'une vulnérabilité connue.")
        return 1

    print(f"\nAucune vulnérabilité connue sur {len(packages)} paquet(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
