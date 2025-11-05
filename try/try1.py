import struct
import mmap
import time
import csv
import os
from typing import List, Dict, Any, Tuple, Optional, Set, Union
from multiprocessing import Pool, cpu_count
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


class MAVParserProcess:
    """Ultra-fast parallel MAVLink log parser - writes each chunk directly to CSV."""

    def __init__(self, file_path: str, type_filter: Optional[List[str]] = None):
        self.file_path = file_path
        self.fmts: Dict[int, Dict[str, Any]] = {}
        self.type_filter = set(type_filter) if type_filter else None
        self.chunks: List[Tuple[int, int]] = []

    def scan_file_and_prepare_chunks(self) -> None:
        self.chunks = []
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
                if offset >= size - 3:
                    break

            self.chunks.append((chunk_start, size))
            mm.close()

    def _parse_fmt(self, chunk: memoryview) -> None:
        fmt_type, fmt_length, name, fmt_str, cols_raw = struct.unpack_from("<BB4s16s64s", chunk, 3)
        name = name.rstrip(b"\x00").decode("ascii", errors="ignore")
        fmt_str = fmt_str.rstrip(b"\x00").decode("ascii", errors="ignore")
        cols = [c.strip() for c in cols_raw.rstrip(b"\x00").decode("ascii", errors="ignore").split(",") if c.strip()]
        combined_fmt = "<" + "".join(FORMAT_TO_STRUCT.get(c, "") for c in fmt_str if c in FORMAT_TO_STRUCT)

        scaling = {col: PRECOMPUTED_SCALES[col] for col in cols if col in PRECOMPUTED_SCALES}
        self.fmts[fmt_type] = {
            "Name": name,
            "Length": fmt_length,
            "Format": fmt_str,
            "Columns": cols,
            "CombinedFmt": combined_fmt,
            "StructSize": struct.calcsize(combined_fmt),
            "Scaling": scaling,
        }

    @staticmethod
    def _process_chunk(args) -> Tuple[int, int]:
        """Each process writes its own CSV file."""
        index, file_path, chunk, fmts, type_filter = args
        start, end = chunk
        output_file = f"chunk_{index}.csv"
        msg_count = 0

        with open(file_path, "rb") as f, open(output_file, "w", newline="", encoding="utf-8") as out:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            mv = memoryview(mm)
            writer = csv.writer(out)
            offset = start

            # Optional: write CSV header on first valid message
            header_written = False

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

                        msg = MAVParserProcess._parse_message(fmt_info, mv, offset + 3)
                        if msg:
                            if not header_written:
                                writer.writerow(msg.keys())
                                header_written = True
                            writer.writerow(msg.values())
                            msg_count += 1

                        offset += length
                        continue
                offset += 1
            del mv
            mm.close()
        return index, msg_count

    @staticmethod
    def _parse_message(fmt_info: Dict[str, Any], mv: Union[bytes, memoryview], payload_offset: int) -> Optional[Dict[str, Any]]:
        message: Dict[str, Any] = {"mavpackettype": fmt_info["Name"]}
        try:
            values = struct.unpack_from(fmt_info["CombinedFmt"], mv, payload_offset)
            for col, val in zip(fmt_info["Columns"], values):
                if col in fmt_info["Scaling"]:
                    val *= fmt_info["Scaling"][col]
                if isinstance(val, bytes):
                    val = val.rstrip(b"\x00").decode("ascii", errors="ignore")
                message[col] = val
            return message
        except Exception:
            return None

    def run(self):
        start = time.perf_counter()
        logger.info("Preparing chunks...")
        self.scan_file_and_prepare_chunks()

        args_list = [
            (i, self.file_path, chunk, self.fmts, self.type_filter)
            for i, chunk in enumerate(self.chunks)
        ]

        with Pool(processes=cpu_count()) as pool:
            results = pool.map(MAVParserProcess._process_chunk, args_list)

        total_msgs = sum(msg_count for _, msg_count in results)
        total_time = time.perf_counter() - start
        logger.info(f"Parsed {total_msgs:,} messages in {total_time:.2f}s ({total_msgs/total_time:,.0f} msg/sec)")

        # Merge CSVs
        with open("all_messages.csv", "w", encoding="utf-8") as out:
            for i in range(len(results)):
                with open(f"chunk_{i}.csv", "r", encoding="utf-8") as f:
                    out.write(f.read())
                os.remove(f"chunk_{i}.csv")


if __name__ == "__main__":
    parser = MAVParserProcess(FILE_PATH)
    parser.run()
