"""Checks that the v1 contract is self-consistent: schemas are valid, examples behave, checksums are current."""

import hashlib
import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

V1 = Path(__file__).resolve().parents[1] / "v1"
SUMS = V1 / "SHA256SUMS"
SCHEMAS = ("market_kline", "news_article", "raw_record")


def _tracked_files() -> list[Path]:
    # Sort on the POSIX string: WindowsPath ordering ignores case, which would differ from Linux CI.
    files = (p for p in V1.rglob("*") if p.is_file() and p != SUMS)
    return sorted(files, key=lambda p: p.relative_to(V1).as_posix())


def compute_sums() -> str:
    lines = []
    for path in _tracked_files():
        # Normalise line endings so Windows checkouts (core.autocrlf) hash the same as Linux.
        digest = hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
        lines.append(f"{digest}  {path.relative_to(V1).as_posix()}")
    return "\n".join(lines) + "\n"


def _load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _validator(name: str) -> Draft202012Validator:
    schema = _load(V1 / f"{name}.schema.json")
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


@pytest.mark.parametrize("name", SCHEMAS)
def test_valid_example_passes(name):
    errors = list(_validator(name).iter_errors(_load(V1 / "examples" / f"{name}.valid.json")))
    assert errors == []


@pytest.mark.parametrize("name", SCHEMAS)
def test_invalid_example_fails(name):
    assert list(_validator(name).iter_errors(_load(V1 / "examples" / f"{name}.invalid.json")))


@pytest.mark.parametrize("value", [
    "2026-10-27T08:00:00Z",          # missing milliseconds
    "2026-10-27T08:00:00.000+07:00",  # not UTC Z
    "2026-10-27 08:00:00.000Z",       # space instead of T
    "1761552000000",                  # epoch ms is not allowed in v1
])
def test_market_rejects_non_canonical_timestamps(value):
    record = _load(V1 / "examples" / "market_kline.valid.json")
    record["open_time"] = value
    assert list(_validator("market_kline").iter_errors(record))


def test_raw_record_value_is_a_valid_market_message():
    raw = _load(V1 / "examples" / "raw_record.valid.json")
    assert list(_validator("market_kline").iter_errors(json.loads(raw["value"]))) == []


def test_checksums_are_current():
    assert SUMS.read_text(encoding="utf-8").replace("\r\n", "\n") == compute_sums(), (
        "contracts/v1 changed: run `python contracts/tests/test_contracts.py --update-sums`"
    )


if __name__ == "__main__":
    if sys.argv[1:] == ["--update-sums"]:
        SUMS.write_text(compute_sums(), encoding="utf-8", newline="\n")
        print("updated contracts/v1/SHA256SUMS")
    else:
        sys.exit("usage: python contracts/tests/test_contracts.py --update-sums")
