from typing import List, Dict, Any, Optional
import struct
import mmap
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.utils.config import (
    HEADER,
    FMT_TYPE,
    FMT_LENGTH,
    FORMAT_TO_STRUCT,
    STRING_FORMATS,
    FIELD_SCALERS,
    FORMAT_SCALERS,
    FILE_PATH,
)


class MAVParser:
    """Ultra-fast MAVLink log parser using memoryview and bit scanning, with proper file closure."""

    def __init__(self, file_path: str, type_filter: Optional[List[str]] = None, rounding: bool = True):
        self.file_path = file_path
        self.fmts: Dict[int, Dict[str, Any]] = {}
        self.message_count = 0
        self.type_filter = set(type_filter) if type_filter else None
        self.rounding = rounding
        self.offset = 0

        # Open file and map to memory
        self._file = open(file_path, "rb")
        self.mm = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
        self.mv = memoryview(self.mm)
        self.size = len(self.mv)

        self.header_bytes = HEADER

        self.rounding_columns: frozenset = frozenset(
            {
                "Lat",
                "Lng",
                "TLat",
                "TLng",
                "Pitch",
                "IPE",
                "Yaw",
                "IPN",
                "IYAW",
                "DesPitch",
                "NavPitch",
                "Temp",
                "AltE",
                "VDop",
                "VAcc",
                "Roll",
                "HAGL",
                "SM",
                "VWN",
                "VWE",
                "IVT",
                "SAcc",
                "TAW",
                "IPD",
                "ErrRP",
                "SVT",
                "SP",
                "TAT",
                "GZ",
                "HDop",
                "NavRoll",
                "NavBrg",
                "TAsp",
                "HAcc",
                "DesRoll",
                "SH",
                "TBrg",
                "AX",
            }
        )

    def __enter__(self) -> "MAVParser":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close memoryview, mmap and file properly."""
        try:
            if hasattr(self, "mv") and self.mv:
                self.mv.release()
            if hasattr(self, "mm") and self.mm:
                self.mm.close()
            if hasattr(self, "_file") and self._file:
                self._file.close()
        except Exception as e:
            print("Error closing file:", e)

    def _make_processor_list(
        self, columns: List[str], format_str: str, name: str
    ) -> List[tuple[str, str, tuple[float, bool]]]:
        procs = []
        for col, fmt_char in zip(columns, format_str):
            if fmt_char == "a":
                procs.append(("array", col, (32,)))
            elif fmt_char in STRING_FORMATS:
                is_data = col == "Data"
                procs.append(("string", col, (is_data,)))
            else:
                scale = FIELD_SCALERS.get(col) or FORMAT_SCALERS.get(fmt_char) or 1.0
                need_round = (col in self.rounding_columns) or (name == "GPS" and col == "Alt")
                procs.append(("numeric", col, (scale, need_round)))
        return procs

    def parse_fmt(self, offset: int) -> Dict[str, Any]:
        mv = self.mv
        fmt_type, fmt_length, name_b, format_b, cols_b = struct.unpack_from("<BB4s16s64s", mv, offset + 3)
        name = name_b.rstrip(b"\x00").decode("ascii", errors="ignore")
        format_str = format_b.rstrip(b"\x00").decode("ascii", errors="ignore")
        columns_raw = cols_b.rstrip(b"\x00").decode("ascii", errors="ignore")
        columns = [c.strip() for c in columns_raw.split(",") if c.strip()]

        struct_fmt = "<" + "".join(FORMAT_TO_STRUCT.get(c, "") for c in format_str if c in FORMAT_TO_STRUCT)
        compiled_struct = struct.Struct(struct_fmt)
        struct_size = compiled_struct.size

        processors = self._make_processor_list(columns, format_str, name)
        self.fmts[fmt_type] = {
            "Name": name,
            "Length": fmt_length,
            "Format": format_str,
            "Columns": columns,
            "CompiledStruct": compiled_struct,
            "StructSize": struct_size,
            "Processors": processors,
        }

        self.message_count += 1
        return {
            "mavpackettype": "FMT",
            "Type": fmt_type,
            "Length": fmt_length,
            "Name": name,
            "Format": format_str,
            "Columns": ",".join(columns),
        }

    def parse_message(self, fmt_type: int, offset: int) -> Optional[Dict[str, Any]]:
        fmt_info = self.fmts.get(fmt_type)
        if not fmt_info:
            return None

        compiled_struct = fmt_info["CompiledStruct"]
        processors = fmt_info["Processors"]
        name = fmt_info["Name"]

        try:
            values = compiled_struct.unpack_from(self.mv, offset)
        except Exception:
            return None

        msg: Dict[str, Any] = {"mavpackettype": name}
        vi = 0
        for kind, col, extra in processors:
            if kind == "array":
                length = extra[0]
                msg[col] = list(values[vi : vi + length])
                vi += length
            elif kind == "string":
                is_data = extra[0]
                val = values[vi]
                vi += 1
                if is_data:
                    msg[col] = val
                else:
                    if isinstance(val, (bytes, bytearray)):
                        msg[col] = val.split(b"\x00", 1)[0].decode("ascii", errors="ignore")
                    else:
                        msg[col] = val
            else:
                scale, need_round = extra
                val = values[vi]
                vi += 1
                if scale != 1.0:
                    val *= scale
                if self.rounding and need_round and isinstance(val, float):
                    val = round(val, 7)
                msg[col] = val

        self.message_count += 1
        return msg

    def parse_one(self) -> Optional[Dict[str, Any]]:
        """
        Return the next message using optimized search:
           - fast 3-byte find if type_filter is active
           - otherwise scan bit by bit
        """
        mv = self.mv
        mm = self.mm
        size = self.size
        offset = self.offset
        h0, h1 = self.header_bytes

        while offset < size - 3:
            if self.type_filter:
                search_bytes_list = [bytes([h0, h1, t]) for t, v in self.fmts.items() if v["Name"] in self.type_filter]

                search_bytes_list.append(bytes([h0, h1, FMT_TYPE]))

                found = False
                for sb in search_bytes_list:
                    pos = mm.find(sb, offset)
                    if pos != -1:
                        offset = pos
                        found = True
                        break
                if not found:
                    self.offset = size
                    return None
            else:

                while offset < size - 1 and not (mv[offset] == h0 and mv[offset + 1] == h1):
                    offset += 1
                if offset >= size - 1:
                    self.offset = size
                    return None

            msg_type = mv[offset + 2]

            if msg_type == FMT_TYPE:
                if offset + FMT_LENGTH > size:
                    self.offset = size
                    return None
                fmt_msg = self.parse_fmt(offset)
                self.offset = offset + FMT_LENGTH
                offset = self.offset
                if self.type_filter is None or "FMT" in self.type_filter:
                    return fmt_msg
                else:
                    continue

            fmt_info = self.fmts.get(msg_type)
            if fmt_info:
                length = fmt_info["Length"]
                if offset + length > size:
                    self.offset = size
                    return None
                if self.type_filter is None or fmt_info["Name"] in self.type_filter:
                    msg = self.parse_message(msg_type, offset + 3)
                    self.offset = offset + length
                    return msg
                else:

                    self.offset = offset + length
                    offset = self.offset
                    continue

            offset += 1

        self.offset = size
        return None

    def parse_all(self) -> List[Any]:
        """Convenience: parse everything at once."""
        msgs: List[Optional[Dict[str, Any]]] = []
        while True:
            msg = self.parse_one()
            if not msg:
                break
            # print(msg)
            msgs.append(msg)
        return msgs


    def print_summary(self) -> None:
        print(f"\n{self.message_count:,} messages parsed in total.")


if __name__ == "__main__":
    import time

    with MAVParser(FILE_PATH) as parser:
        start = time.perf_counter()
        # all_msgs = parser.parse_all()
        all_msgs = parser.parse_all()
        print(f"\nRuntime: {time.perf_counter() - start:.2f} seconds")

    from pymavlink import mavutil

    start_mav = time.perf_counter()
    mav = mavutil.mavlink_connection(FILE_PATH)
    i = 0
    l = []
    while True:
        msg = mav.recv_match(blocking=False)
        if msg is None:
            break
        l.append(msg)
    print(f"mav {time.perf_counter() - start_mav}")
