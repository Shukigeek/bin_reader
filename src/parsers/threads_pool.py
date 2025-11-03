import struct
import mmap
import time
from typing import List, Dict, Any, Tuple, Optional, Set, Union
from concurrent.futures import ThreadPoolExecutor
import os

from src.utils.logger import logger

from src.utils.config import (
    HEADER,
    FMT_HEADER,
    FMT_LENGTH,
    FORMAT_TO_STRUCT,
    STRING_FORMATS,
    PRECOMPUTED_SCALES,
    FILE_PATH,
)


class MAVParserHybrid:
    """First run to find all FMT messages and create safe chunks.
    Then all processes run on a chunk from the bin file
    and return a list of parsed message dictionaries."""

    def __init__(self, file_path: str, type_filter: Optional[List[str]] = None):
        self.file_path = file_path
        self.fmts: Dict[int, Dict[str, Any]] = {}
        self.type_filter = set(type_filter) if type_filter else None
        self.message_count = 0
        self.chunks: List[Tuple[int, int]] = []
        self.messages: List[Dict[str, Any]] = []

        self.rounding_columns = {
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

    # def scan_file_and_prepare_chunks(self) -> None:
    #     """First run to find all FMT messages and create (safe) chunks."""
    #     self.chunks = []
    #
    #     with open(self.file_path, "rb") as f:
    #         mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    #         mv = memoryview(mm)
    #         size = len(mv)
    #         offset = 0
    #
    #         num_procs = cpu_count()
    #         chunk_size = size // num_procs
    #         desired_cuts = [(i + 1) * chunk_size for i in range(num_procs - 1)]
    #         current_cut_idx = 0
    #         chunk_start = 0
    #
    #         while offset < size - 3:
    #             if mv[offset] == HEADER[0] and mv[offset + 1] == HEADER[1]:
    #                 msg_type = mv[offset + 2]
    #
    #                 if msg_type == FMT_TYPE and offset + FMT_LENGTH <= size:
    #                     self._parse_fmt(mv[offset : offset + FMT_LENGTH])
    #                     offset += FMT_LENGTH
    #                     continue
    #
    #                 elif msg_type in self.fmts:
    #                     msg_len = self.fmts[msg_type]["Length"]
    #
    #                     if (
    #                         offset + msg_len < size
    #                         and mv[offset + msg_len] == HEADER[0]
    #                         and mv[offset + msg_len + 1] == HEADER[1]
    #                     ):
    #
    #                         if current_cut_idx < len(desired_cuts) and offset >= desired_cuts[current_cut_idx]:
    #                             self.chunks.append((chunk_start, offset))
    #                             chunk_start = offset
    #                             current_cut_idx += 1
    #                         offset += msg_len
    #                         continue
    #
    #             offset += 1
    #
    #         self.chunks.append((chunk_start, size))
    #         del mv
    #         mm.close()
    def scan_file_and_prepare_chunks(self) -> None:
        """Scan head for FMTs, then find safe cuts from estimated points."""
        self.chunks = []
        # header_bytes = bytes(HEADER[:2])
        # fmt_header = header_bytes + bytes([FMT_TYPE])

        with open(self.file_path, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            size = len(mm)

            # Stage 1: Fast FMT scan in head
            offset = 0
            max_head = min(size, 50_000_000)  # Scan only first 50MB for FMTs
            while offset < max_head - 3:
                pos = mm.find(FMT_HEADER, offset)
                if pos == -1 or pos >= max_head:
                    break
                if pos + FMT_LENGTH <= size:
                    self._parse_fmt(mm[pos: pos + FMT_LENGTH])
                    offset = pos + FMT_LENGTH
                else:
                    offset += 1

            logger.info(f"Found {len(self.fmts)} FMT definitions in head")

            # Stage 2: Prepare safe chunks from estimated cuts
            num_procs = min(os.cpu_count() or 8, 16)
            chunk_size = size // num_procs
            desired_cuts = [chunk_size * (i + 1) for i in range(num_procs - 1)]
            chunk_start = 0

            for cut in desired_cuts:
                # Start from estimated cut, find next valid message start
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
                            # Found safe cut at message boundary
                            self.chunks.append((chunk_start, offset))
                            chunk_start = offset
                            break
                    offset += 1  # Byte-by-byte if not found quickly
                if offset >= size - 3:
                    break  # No more cuts

            # Last chunk
            self.chunks.append((chunk_start, size))
            mm.close()

            logger.info(f"Prepared {len(self.chunks)} safe chunks")
    def _parse_fmt(self, chunk: memoryview) -> None:
        """Creating one fmt setting for all types of messages."""
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

    @staticmethod
    def _process_chunk(args) -> Tuple[int, List[Optional[Dict[str, Any]]]]:
        """Worker function for Pool. Returns (index, list of messages)."""
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

                        # MAVParserHybrid._parse_message(fmt_info, mv, offset + 3, rounding)
                        message = MAVParserHybrid._parse_message(fmt_info, mv, offset + 3, rounding)
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
        """strings / arrays / scaling / rounding"""
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

                if rounding and isinstance(val, float) and col in fmt_info["Rounding"]:
                    val = round(val, 7)
                if fmt_info["Name"] == "GPS" and col == "Alt":
                    val = round(val, 7)

                message[col] = val

            return message

        except Exception as e:
            print(f"Error parsing {fmt_info.get('Name', '?')}: {e}")
            return None

    def run(self, rounding: bool = True) -> None:
        start_total = time.perf_counter()
        logger.info("Scanning file and preparing chunks...")
        self.scan_file_and_prepare_chunks()
        scan_time = time.perf_counter() - start_total
        logger.info(f"Found {len(self.fmts)} FMT definitions, {len(self.chunks)} chunks")
        logger.info(f"Scan & chunk preparation time: {scan_time:.2f} s\n")

        args_list = [
            (i, self.file_path, chunk, self.fmts, self.type_filter, rounding)
            for i, chunk in enumerate(self.chunks)
        ]

        logger.info("Starting thread pool...")
        start_pool = time.perf_counter()

        max_workers = min(os.cpu_count() or 8, 16)
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(MAVParserHybrid._process_chunk, args) for args in args_list]
            for f in futures:
                results.append(f.result())

        # Sort results by chunk index to maintain file order
        results.sort(key=lambda x: x[0])

        # Flatten all messages from all chunks
        self.messages = [msg for _, msgs in results for msg in msgs]
        self.message_count = len(self.messages)

        pool_time = time.perf_counter() - start_pool
        total_time = time.perf_counter() - start_total

        logger.info(f"\nTotal messages parsed: {self.message_count:,}")
        logger.info(f"Thread pool time: {pool_time:.2f} s, Total: {total_time:.2f} s")
        if total_time > 0:
            logger.info(f"{self.message_count / total_time:,.0f} msgs/sec")


if __name__ == "__main__":
    parser = MAVParserHybrid(FILE_PATH)
    parser.run()
    # Access messages if needed:
    print(parser.messages[:5])
