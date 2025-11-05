import json
from pathlib import Path

# ---------- Load config from JSON ----------
CONFIG_PATH = r"C:\Users\shuki\Desktop\9900\bin_reader\config.json"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    raw_config = json.load(f)

# ---------- File & Logger Settings ----------
FILE_PATH = raw_config["FILE_PATH"]
LOGGER_SETTINGS = raw_config["LOGGER_SETTINGS"]

# ---------- Binary Format Config ----------
binary_config = raw_config["BINARY_FORMAT"]

HEADER = bytes.fromhex(binary_config["HEADER"])
FMT_TYPE = binary_config["FMT_TYPE"]
FMT_HEADER = bytes.fromhex(binary_config["FMT_HEADER"])
FMT_LENGTH = binary_config["FMT_LENGTH"]

FORMAT_TO_STRUCT = binary_config["FORMAT_TO_STRUCT"]
FMT_SIZE_MAP = binary_config["FMT_SIZE_MAP"]

# ---------- Scaling ----------
FIELD_SCALERS = binary_config.get("FIELD_SCALERS", {})
FORMAT_SCALERS = binary_config.get("FORMAT_SCALERS", {})
PRECOMPUTED_SCALES = {**FIELD_SCALERS, **FORMAT_SCALERS}

# ---------- String formats ----------
STRING_FORMATS = frozenset(binary_config.get("STRING_FORMATS", []))

# ---------- Rounding ----------
ROUNDING = frozenset(binary_config.get("ROUNDING", []))

# ---------- Precompiled Structs ----------
import struct
STRUCT_CACHE = {fmt: struct.Struct("<" + fmt_str) for fmt, fmt_str in FORMAT_TO_STRUCT.items()}
