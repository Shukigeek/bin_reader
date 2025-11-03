import json
import queue
import struct
from typing import Dict, Optional

# ---------- Queue ----------
points_queue: queue.Queue[Optional[Dict[str, float]]] = queue.Queue()

# ---------- Load config ----------
with open(r"C:\Users\shuki\Desktop\9900\bin_reader\config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

FILE_PATH = config["FILE_PATH"]
LOGGER_SETTINGS = config["LOGGER_SETTINGS"]

# ---------- Binary Format Config ----------
HEADER = b"\xa3\x95"
FMT_TYPE = 0x80
FMT_HEADER = b"\xa3\x95\x80"
FMT_LENGTH = 89

FORMAT_TO_STRUCT = {
    "a": "32h",  # int16_t[32]
    "b": "b",  # int8_t
    "B": "B",  # uint8_t
    "h": "h",  # int16_t
    "H": "H",  # uint16_t
    "i": "i",  # int32_t
    "I": "I",  # uint32_t
    "f": "f",  # float
    "d": "d",  # double
    "n": "4s",  # char[4]
    "N": "16s",  # char[16]
    "Z": "64s",  # char[64]
    "c": "h",  # int16_t * 100
    "C": "H",  # uint16_t * 100
    "e": "i",  # int32_t * 100
    "E": "I",  # uint32_t * 100
    "L": "i",  # int32_t lat/lng * 1e-7
    "M": "B",  # flight mode
    "q": "q",  # int64_t
    "Q": "Q",  # uint64_t
}

FMT_SIZE_MAP = {
    "a": 64,
    "b": 1,
    "B": 1,
    "h": 2,
    "H": 2,
    "i": 4,
    "I": 4,
    "f": 4,
    "d": 8,
    "n": 4,
    "N": 16,
    "Z": 64,
    "c": 2,
    "C": 2,
    "e": 4,
    "E": 4,
    "L": 4,
    "M": 1,
    "q": 8,
    "Q": 8,
}

# ---------- Performance Optimizations ----------

# Pre-compile all struct formats for faster unpacking
STRUCT_CACHE = {fmt: struct.Struct("<" + struct_fmt) for fmt, struct_fmt in FORMAT_TO_STRUCT.items()}

# Field-specific scaling factors
FIELD_SCALERS = {"HDop": 1e-2, "Lat": 1e-7, "Lng": 1e-7, "TLat": 1e-7, "TLng": 1e-7}

# Format-specific scaling factors
FORMAT_SCALERS = {"c": 1e-2, "C": 1e-2, "e": 1e-2, "E": 1e-2}  # int * 100

# String format characters (no unpacking needed)
STRING_FORMATS = frozenset({"n", "N", "Z"})

# ---------- Precomputed scaling ----------
PRECOMPUTED_SCALES = {**FIELD_SCALERS, **FORMAT_SCALERS}
