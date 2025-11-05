import struct
import mmap
from typing import List, Dict, Any, Tuple, Optional, Set, Union
from multiprocessing import Pool, cpu_count
# from src.utils.logger import logger

from src.utils.config import (
    HEADER,
    FMT_HEADER,
    FMT_LENGTH,
    FORMAT_TO_STRUCT,
    STRING_FORMATS,
    PRECOMPUTED_SCALES,
    FILE_PATH,
    ROUNDING,
)


class MAVParserProcess:
    """Parse binary MAV messages using multiple processes."""

    def __init__(self, file_path: str, type_filter: Optional[List[str]] = None):
        self.file_path = file_path
        self.fmts: Dict[int, Dict[str, Any]] = {}
        self.type_filter = set(type_filter) if type_filter else None
        self.chunks: List[Tuple[int, int]] = []
        self.messages: List[Dict[str, Any]] = []

        self.rounding_columns : frozenset[str] = ROUNDING

    def scan_file_and_prepare_chunks(self) -> None:
        self._scan_fmts()
        self._prepare_safe_chunks()

    def _scan_fmts(self) -> None:
        """Scan file head for FMT definitions."""
        with open(self.file_path, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            size = len(mm)
            offset = 0
            max_head = min(size, 50_000_000)

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
        # logger.info(f"Found {len(self.fmts)} FMT definitions")

    def _parse_fmt(self, chunk: memoryview) -> None:
        fmt_type, fmt_length, name, fmt_str, cols_raw = struct.unpack_from("<BB4s16s64s", chunk, 3)
        name = name.rstrip(b"\x00").decode("ascii", errors="ignore")
        fmt_str = fmt_str.rstrip(b"\x00").decode("ascii", errors="ignore")
        cols = [c.strip() for c in cols_raw.rstrip(b"\x00").decode("ascii", errors="ignore").split(",") if c.strip()]
        combined_fmt = "<" + "".join(FORMAT_TO_STRUCT.get(c, "") for c in fmt_str if c in FORMAT_TO_STRUCT)

        scaling = {col: PRECOMPUTED_SCALES[col] for col in cols if col in PRECOMPUTED_SCALES}
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
        """Split the file into safe chunks for multiprocessing."""
        with open(self.file_path, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            size = len(mm)

            num_procs = min(cpu_count(), 16)
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

            self.chunks.append((chunk_start, size))
            mm.close()
        # logger.info(f"Prepared {len(self.chunks)} safe chunks")

    @staticmethod
    def _process_chunk(args) -> Tuple[int, List[Optional[Dict[str, Any]]]]:
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
                        message = MAVParserProcess._parse_message(fmt_info, mv, offset + 3, rounding)
                        if message:
                            messages.append(message)
                        offset += length
                        continue
                offset += 1
            # messages = []
            del mv
            mm.close()
        return index, messages

    @staticmethod
    def _parse_message(
        fmt_info: Dict[str, Any], mv: Union[bytes, memoryview], payload_offset: int, rounding: Set[str]
    ) -> Optional[Dict[str, Any]]:
        """Parse a single message into dict."""
        message: Dict[str, Any] = {"mavpackettype": fmt_info["Name"]}
        try:
            values = struct.unpack_from(fmt_info["CombinedFmt"], mv, payload_offset)
            for idx, (col, fmt_char) in enumerate(zip(fmt_info["Columns"], fmt_info["Format"])):
                val = values[idx]
                if fmt_char in STRING_FORMATS and isinstance(val, bytes):
                    val = val if col == "Data" else val.rstrip(b"\x00").decode("ascii", errors="ignore")
                if col in fmt_info["Scaling"]:
                    val *= fmt_info["Scaling"][col]
                if rounding and isinstance(val, float) and col in fmt_info["Rounding"]:
                    val = round(val, 7)
                if fmt_info["Name"] == "GPS" and col == "Alt":
                    val = round(val, 7)
                message[col] = val
            return message
        except Exception as e:
            # logger.error(f"Error parsing {fmt_info.get('Name', '?')}: {e}")
            return None

    def run(self, rounding: bool = True) -> None:
        self.scan_file_and_prepare_chunks()
        args_list = [
            (i, self.file_path, chunk, self.fmts, self.type_filter, rounding) for i, chunk in enumerate(self.chunks)
        ]

        with Pool(processes=cpu_count()) as pool:
            results = pool.map(MAVParserProcess._process_chunk, args_list)

        results.sort(key=lambda x: x[0])
        self.messages = [msg for _, msgs in results for msg in msgs]
        self.message_count = len(self.messages)


if __name__ == "__main__":
    import time
    start = time.perf_counter()
    parser = MAVParserProcess(FILE_PATH)
    parser.run()
    print(parser.messages[:5])
    print(f"process time {time.perf_counter() - start}")
