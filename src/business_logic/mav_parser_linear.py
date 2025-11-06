from typing import List, Dict, Any, Optional
import struct
import mmap
from src.utils.config import (
    HEADER,
    FMT_TYPE,
    FMT_LENGTH,
    FORMAT_TO_STRUCT,
    STRING_FORMATS,
    FIELD_SCALERS,
    FORMAT_SCALERS,
    FILE_PATH,
    ROUNDING,
)


class MAVParserLinear:
    """Ultra-fast MAVLink log parser using memoryview and mmap."""

    def __init__(self, file_path: str, type_filter: Optional[List[str]] = None, rounding: bool = True):
        self.file_path = file_path
        self.formats: Dict[int, Dict[str, Any]] = {}
        self.message_count = 0
        self.type_filter = set(type_filter) if type_filter else None
        self.rounding = rounding
        self.offset = 0
        self.header_bytes = HEADER
        self.columns_to_round = ROUNDING

        # Open file and map to memory
        self._file = open(file_path, "rb")
        self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
        self._view = memoryview(self._mmap)
        self.size = len(self._view)

    def __enter__(self) -> "MAVParser":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close all resources properly."""
        try:
            if hasattr(self, "_view"):
                self._view.release()
            if hasattr(self, "_mmap"):
                self._mmap.close()
            if hasattr(self, "_file"):
                self._file.close()
        except Exception as e:
            print(f"Error closing: {e}")

    def _build_processors(self, columns: List[str], format_str: str):
        """Build simple processors: type, column, scale."""
        processors = []
        for col, fmt_char in zip(columns, format_str):
            if fmt_char == "a":
                processors.append(("array", col, 32))
            elif fmt_char in STRING_FORMATS:
                processors.append(("string", col, 0))
            else:
                scale = FIELD_SCALERS.get(col) or FORMAT_SCALERS.get(fmt_char) or 1.0
                processors.append(("numeric", col, scale))
        return processors

    def _parse_fmt(self, offset: int) -> Dict[str, Any]:
        """Parse FMT message that defines message format."""
        fmt_type, fmt_length, name_b, format_b, cols_b = struct.unpack_from(
            "<BB4s16s64s", self._view, offset + 3
        )

        name = name_b.rstrip(b"\x00").decode("ascii", errors="ignore")
        format_str = format_b.rstrip(b"\x00").decode("ascii", errors="ignore")
        columns_raw = cols_b.rstrip(b"\x00").decode("ascii", errors="ignore")
        columns = [c.strip() for c in columns_raw.split(",") if c.strip()]

        struct_fmt = "<" + "".join(FORMAT_TO_STRUCT.get(c, "") for c in format_str if c in FORMAT_TO_STRUCT)
        compiled_struct = struct.Struct(struct_fmt)
        processors = self._build_processors(columns, format_str)

        self.formats[fmt_type] = {
            "Name": name,
            "Length": fmt_length,
            "CompiledStruct": compiled_struct,
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

    def _parse_message(self, fmt_type: int, offset: int) -> Optional[Dict[str, Any]]:
        """Parse regular data message."""
        fmt_info = self.formats.get(fmt_type)
        if not fmt_info:
            return None

        try:
            values = fmt_info["CompiledStruct"].unpack_from(self._view, offset)
        except Exception:
            return None

        msg = {"mavpackettype": fmt_info["Name"]}
        value_idx = 0

        for kind, col, scale in fmt_info["Processors"]:
            val = values[value_idx]
            value_idx += 1

            if kind == "array":
                msg[col] = list(values[value_idx - 1 : value_idx - 1 + scale])
                value_idx += scale - 1
            elif kind == "string":
                msg[col] = val.decode("ascii", errors="ignore") if isinstance(val, (bytes, bytearray)) else val
            else:  # numeric
                if scale != 1.0:
                    val *= scale
                if self.rounding and isinstance(val, float):
                    if col in self.columns_to_round or (fmt_info["Name"] == "GPS" and col == "Alt"):
                        val = round(val, 7)
                msg[col] = val

        self.message_count += 1
        return msg

    def _find_next_header(self) -> Optional[int]:
        """Find next message header in file."""
        h0, h1 = self.header_bytes

        while self.offset < self.size - 1:
            if self._view[self.offset] == h0 and self._view[self.offset + 1] == h1:
                return self.offset
            self.offset += 1
        return None

    def parse_next(self) -> Optional[Dict[str, Any]]:
        """Return next message from file."""
        while self.offset < self.size - 3:
            header_pos = self._find_next_header()
            if header_pos is None:
                self.offset = self.size
                return None

            self.offset = header_pos
            msg_type = self._view[self.offset + 2]

            if msg_type == FMT_TYPE:
                if self.offset + FMT_LENGTH > self.size:
                    self.offset = self.size
                    return None
                fmt_msg = self._parse_fmt(self.offset)
                self.offset += FMT_LENGTH
                if self.type_filter is None or "FMT" in self.type_filter:
                    return fmt_msg
                continue

            fmt_info = self.formats.get(msg_type)
            if fmt_info:
                length = fmt_info["Length"]
                if self.offset + length > self.size:
                    self.offset = self.size
                    return None
                if self.type_filter is None or fmt_info["Name"] in self.type_filter:
                    msg = self._parse_message(msg_type, self.offset + 3)
                    self.offset += length
                    return msg
                self.offset += length
                continue

            self.offset += 1

        self.offset = self.size
        return None

    def parse_all(self) -> List[Dict[str, Any]]:
        messages = []
        while msg := self.parse_next():
            messages.append(msg)
        return messages

    def print_summary(self) -> None:
        print(f"\n{self.message_count:,} messages parsed.")


if __name__ == "__main__":
    import time
    from pymavlink import mavutil

    with MAVParserLinear(FILE_PATH,type_filter=["GPS"]) as parser:
        start = time.perf_counter()
        all_msgs = parser.parse_all()
        print(f"Runtime: {time.perf_counter() - start:.2f}s")
        parser.print_summary()

        # start_mav = time.perf_counter()
        # mav = mavutil.mavlink_connection(FILE_PATH)
        # i = 0
        # while True:
        #     msg = mav.recv_match(blocking=False, type=["GPS"])
        #     my_msg = parser.parse_next()
        #
        #     if msg is None:
        #         break
        #     if msg.to_dict() != my_msg:
        #         if "Default" in msg.to_dict():
        #             continue
        #         print("Mismatch at index:", i)
        #         print("pymavlink:", msg.to_dict())
        #         print("MAVParser:", my_msg)
        #         break
        #     if i % 1_000_000 == 0:
        #         print("Checked:", i)
        #     i += 1
