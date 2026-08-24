"""Génère le lot de wheels à uploader dans Files/libs du lakehouse Fabric :
le wheel de flume-lib + toutes ses dépendances résolues pour le kernel Fabric
(Linux x86_64), les empreintes SHA256 et un zip prêt à transférer.

Le lot doit être construit **par l'interpréteur de la version visée**.
`pip download --python-version` ne pilote que les tags de wheel et
`Requires-Python` ; les marqueurs d'environnement (`typing-extensions ;
python_full_version < "3.12"`) restent évalués contre l'interpréteur qui
tourne. Construire un lot 3.11 depuis un poste en 3.14 donne donc les bons
wheels compilés, mais un lot amputé des dépendances conditionnelles — et
`pip install --no-index` échoue chez le client. Le script refuse par défaut.

Usage :
    python3.12 scripts/build_fabric_wheels.py           # lot pour kernel 3.12
    python3.11 scripts/build_fabric_wheels.py           # lot pour kernel 3.11
    python scripts/build_fabric_wheels.py --python-version 3.11 --allow-marker-mismatch

Install côté notebook (après upload des .whl dans le dossier de wheels) :
    %pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib==X.Y.Z
"""

import argparse
import hashlib
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Tags de plateforme des kernels Fabric (Linux x86_64) ; plusieurs tags car
# chaque projet publie sous une variante manylinux différente
MANYLINUX_PLATFORMS = [
    "manylinux_2_28_x86_64",
    "manylinux_2_17_x86_64",
    "manylinux2014_x86_64",
]


def run(cmd: list[str]) -> None:
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=REPO_ROOT)


def build_lib_wheel() -> Path:
    dist = REPO_ROOT / "dist"
    shutil.rmtree(dist, ignore_errors=True)
    run([sys.executable, "-m", "build", "--wheel"])
    wheels = list(dist.glob("flume_lib-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"Attendu un seul wheel dans {dist}, trouvé : {wheels}")
    return wheels[0]


def download_dependencies(lib_wheel: Path, out_dir: Path, python_version: str) -> None:
    cmd = [
        sys.executable, "-m", "pip", "download", str(lib_wheel),
        "-d", str(out_dir),
        "--only-binary=:all:",
        "--python-version", python_version,
        "--implementation", "cp",
    ]
    for platform in MANYLINUX_PLATFORMS:
        cmd += ["--platform", platform]
    run(cmd)


def write_checksums(out_dir: Path) -> Path:
    lines = []
    for wheel in sorted(out_dir.glob("*.whl")):
        digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
        lines.append(f"{digest}  {wheel.name}")
    checksums = out_dir / "SHA256SUMS.txt"
    # newline="\n" impératif : généré sous Windows, le fichier partirait en
    # CRLF et `sha256sum -c` le rejetterait ligne par ligne côté Linux — il ne
    # pourrait plus servir à ce pour quoi il existe.
    checksums.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    return checksums


def make_zip(out_dir: Path, version: str, python_version: str) -> Path:
    # La version de kernel dans le nom : deux lots pour deux kernels sont
    # indiscernables autrement, et se déposer l'un pour l'autre ne se voit
    # qu'à l'installation.
    tag = "py" + python_version.replace(".", "")[:3]
    zip_path = out_dir / f"flume-lib-{version}-fabric-wheels-{tag}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for file in sorted(out_dir.glob("*.whl")) + [out_dir / "SHA256SUMS.txt"]:
            archive.write(file, file.name)
    return zip_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    running = f"{sys.version_info.major}.{sys.version_info.minor}"
    parser.add_argument(
        "--python-version", default=running,
        help=f"Version Python du kernel Fabric cible (défaut : {running}, "
             "celle de l'interpréteur courant)",
    )
    parser.add_argument(
        "--allow-marker-mismatch", action="store_true",
        help="Construire malgré une version cible différente de l'interpréteur "
             "courant. Le lot risque d'être amputé de dépendances "
             "conditionnelles — voir la docstring.",
    )
    parser.add_argument(
        "--out", default="fabric-wheels",
        help="Dossier de sortie, relatif à la racine du repo (défaut : fabric-wheels)",
    )
    args = parser.parse_args()

    target = ".".join(args.python_version.split(".")[:2])
    if target != running and not args.allow_marker_mismatch:
        raise SystemExit(
            f"Interpréteur courant en {running}, lot demandé pour {target}.\n"
            "pip évalue les marqueurs d'environnement contre l'interpréteur qui "
            "tourne, pas contre --python-version : le lot serait amputé des "
            "dépendances conditionnelles à la version (arro3-core exige "
            "typing-extensions en dessous de 3.12) et l'installation hors ligne "
            "échouerait chez le client.\n\n"
            f"Relancer avec un interpréteur {target}, ou passer "
            "--allow-marker-mismatch en connaissance de cause."
        )

    out_dir = REPO_ROOT / args.out
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)

    lib_wheel = build_lib_wheel()
    version = lib_wheel.name.split("-")[1]
    shutil.copy2(lib_wheel, out_dir)
    download_dependencies(lib_wheel, out_dir, args.python_version)
    write_checksums(out_dir)
    zip_path = make_zip(out_dir, version, args.python_version)

    print(f"\nflume-lib {version} — kernel Python {args.python_version} :")
    for wheel in sorted(out_dir.glob("*.whl")):
        print(f"  {wheel.name}")
    print(f"\nZip : {zip_path}")
    print(
        "\nUploader les .whl dans le dossier de wheels du lakehouse, puis :\n"
        "  %pip install --no-index --find-links=/lakehouse/default/Files/libs "
        f"flume-lib=={version}"
    )


if __name__ == "__main__":
    main()
