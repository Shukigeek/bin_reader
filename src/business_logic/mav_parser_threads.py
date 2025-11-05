import struct
import mmap
from typing import List, Dict, Any, Tuple, Optional, Set, Union
from concurrent.futures import ThreadPoolExecutor
import os

# from src.utils.logger import logger
from src.utils.config import (
    HEADER,
    FMT_HEADER,
    FMT_LENGTH,
    FORMAT_TO_STRUCT,
    STRING_FORMATS,
    PRECOMPUTED_SCALES,
    FILE_PATH,
    FIELD_SCALERS,
    FORMAT_SCALERS,
    ROUNDING,
)


class MAVParserThreads:
    def __init__(self, file_path: str, type_filter: Optional[List[str]] = None):
        self.file_path = file_path
        self.fmts: Dict[int, Dict[str, Any]] = {}
        self.type_filter = set(type_filter) if type_filter else None
        self.messages: List[Dict[str, Any]] = []
        self.chunks: List[Tuple[int, int]] = []

        self.rounding_columns : frozenset[str] = ROUNDING

    def scan_file_and_prepare_chunks(self) -> None:
        self._scan_fmts()
        self._prepare_safe_chunks()

    def _scan_fmts(self) -> None:
        """Scan file head for FMT definitions and parse them."""
        with open(self.file_path, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            size = len(mm)
            offset = 0
            max_head = min(size, 50_000_000)  # Scan only first 50MB

            while offset < max_head - 3:
                pos = mm.find(FMT_HEADER, offset)
                if pos == -1 or pos >= max_head:
                    break
                if pos + FMT_LENGTH <= size:
                    self._parse_fmt(mm[pos: pos + FMT_LENGTH])
                    offset = pos + FMT_LENGTH
                else:
                    offset += 1

            mm.close()
        # logger.info(f"Found {len(self.fmts)} FMT definitions in head")

    def _parse_fmt(self, chunk: memoryview) -> None:
        """Parse one FMT definition and store it in self.fmts."""
        fmt_type, fmt_length, name, fmt_str, cols_raw = struct.unpack_from("<BB4s16s64s", chunk, 3)
        name = name.rstrip(b"\x00").decode("ascii", errors="ignore")
        fmt_str = fmt_str.rstrip(b"\x00").decode("ascii", errors="ignore")
        cols = [c.strip() for c in cols_raw.rstrip(b"\x00").decode("ascii", errors="ignore").split(",") if c.strip()]
        combined_fmt = "<" + "".join(FORMAT_TO_STRUCT.get(c, "") for c in fmt_str if c in FORMAT_TO_STRUCT)

        scaling = {}
        for col, fmt_char in zip(cols, fmt_str):
            scaling[col] = FIELD_SCALERS.get(col) or FORMAT_SCALERS.get(fmt_char) or 1
        rounding = self.rounding_columns.intersection(cols)

        self.fmts[fmt_type] = {
            "Name": name,
            "Length": fmt_length,
            "Format": fmt_str,
            "Columns": cols,
            "CombinedFmt": combined_fmt,
            "StructSize": struct.calcsize(combined_fmt),
            "Scaling": scaling,
            "Rounding": rounding,
        }

    def _prepare_safe_chunks(self) -> None:
        """Split the file into safe chunks for multithreading."""
        with open(self.file_path, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            size = len(mm)

            num_procs = min(os.cpu_count() or 8, 16)
            chunk_size = size // num_procs
            desired_cuts = [chunk_size * (i + 1) for i in range(num_procs - 1)]
            chunk_start = 0

            for cut in desired_cuts:
                offset = max(cut, chunk_start)
                while offset < size - 3:
                    pos = mm.find(HEADER, offset)
                    if pos == -1:
                        break
                    offset = pos
                    msg_type = mm[offset + 2]
                    if msg_type in self.fmts:
                        msg_len = self.fmts[msg_type]["Length"]
                        if offset + msg_len <= size:
                            self.chunks.append((chunk_start, offset))
                            chunk_start = offset
                            break
                    offset += 1
                if offset >= size - 3:
                    break

            self.chunks.append((chunk_start, size))
            mm.close()
        # logger.info(f"Prepared {len(self.chunks)} safe chunks")

    def _process_chunk(self, args) -> Tuple[int, List[Optional[Dict[str, Any]]]]:
        index, file_path, chunk, fmts, type_filter, rounding = args
        start, end = chunk
        messages: List[Any] = []

        with open(file_path, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            mv = memoryview(mm)
            offset = start

            while offset < end - 3:
                if mv[offset] == HEADER[0] and mv[offset + 1] == HEADER[1]:
                    msg_type = mv[offset + 2]
                    if msg_type in fmts:
                        fmt_info = fmts[msg_type]
                        length = fmt_info["Length"]
                        if offset + length > end:
                            break
                        if type_filter and fmt_info["Name"] not in type_filter:
                            offset += length
                            continue
                        message = self._parse_message(fmt_info, mv, offset + 3, rounding)
                        if message:
                            messages.append(message)
                        offset += length
                        continue
                offset += 1

            del mv
            mm.close()
        return index, messages

    @staticmethod
    def _parse_message(
        fmt_info: Dict[str, Any], mv: Union[bytes, memoryview], payload_offset: int, rounding: Set[str]
    ) -> Optional[Dict[str, Any]]:
        """Parse a single message into dict with scaling, rounding, strings."""
        message: Dict[str, Any] = {"mavpackettype": fmt_info["Name"]}
        try:
            values = struct.unpack_from(fmt_info["CombinedFmt"], mv, payload_offset)
            idx = 0
            for col, fmt_char in zip(fmt_info["Columns"], fmt_info["Format"]):
                val = values[idx]
                idx += 1
                if fmt_char in STRING_FORMATS and isinstance(val, bytes):
                    message[col] = val if col == "Data" else val.rstrip(b"\x00").decode("ascii", errors="ignore")
                    continue
                if col in fmt_info["Scaling"]:
                    val *= fmt_info["Scaling"][col]
                if isinstance(val, float):
                    if (rounding and col in fmt_info["Rounding"]) or (fmt_info["Name"] == "GPS" and col == "Alt"):
                        val = round(val, 7)
                message[col] = val
            return message
        except Exception as e:
            # logger.error(f"Error parsing {fmt_info.get('Name', '?')}: {e}")
            return None


    def run(self, rounding: bool = True) -> None:
        self.scan_file_and_prepare_chunks()
        args_list = [
            (i, self.file_path, chunk, self.fmts, self.type_filter, rounding)
            for i, chunk in enumerate(self.chunks)
        ]

        max_workers = min(os.cpu_count() or 8, 16)
        results: List[Tuple[int, List[Dict[str, Any]]]] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self._process_chunk, args) for args in args_list]
            for f in futures:
                results.append(f.result())

        results.sort(key=lambda x: x[0])
        self.messages = [msg for _, msgs in results for msg in msgs]



if __name__ == "__main__":
    parser = MAVParserThreads(FILE_PATH,type_filter=["GPS"])
    parser.run()
    print(parser.messages[:1])

    from pymavlink import mavutil

    mav = mavutil.mavlink_connection(FILE_PATH)
    msg = mav.recv_match(blocking=False,type=["GPS"])
    # for i in range(10):
    #     msg = mav.recv_match(blocking=False, type=["FMT", "GPS"])
    print(msg.to_dict())

