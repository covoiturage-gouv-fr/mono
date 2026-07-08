import hashlib
import os

# Algos acceptés pour *vérifier* une empreinte : on refuse md5/sha1 (collisions praticables)
# pour qu'un algo faible ne puisse pas s'auto-autoriser via le préfixe stocké en config.
_ALLOWED_ALGOS = {"sha256", "sha512"}


def verify_size(path: str, expected: int, label: str) -> None:
  """Échoue si la taille du fichier diffère (détection rapide de troncature, avant le hash)."""
  actual = os.path.getsize(path)
  if actual != expected:
    raise RuntimeError(f"❌ Taille {label} invalide : attendu {expected}, obtenu {actual} octets")


def hash_file(path: str, algo: str = "sha256") -> str:
  """Empreinte d'un fichier, lue en flux (mémoire bornée quelle que soit la taille)."""
  h = hashlib.new(algo)
  with open(path, "rb") as f:
    for chunk in iter(lambda: f.read(1 << 20), b""):
      h.update(chunk)
  return h.hexdigest()


def verify_checksum(path: str, expected: str, label: str) -> None:
  """Échoue si l'empreinte ne correspond pas. `expected` est préfixé par l'algo : « sha256:abc… »."""
  algo, _, want = expected.partition(":")
  if not want:
    raise RuntimeError(f"❌ Checksum {label} mal formé (attendu « <algo>:<hex> ») : {expected}")
  if algo not in _ALLOWED_ALGOS:
    raise RuntimeError(f"❌ Checksum {label} : algo « {algo} » non autorisé (attendu sha256 ou sha512)")
  actual = hash_file(path, algo)
  if actual != want:
    raise RuntimeError(f"❌ Checksum {label} ({algo}) invalide : attendu {want}, obtenu {actual}")
