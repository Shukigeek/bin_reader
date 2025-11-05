# import struct
# import mmap
# import time
# from typing import List, Dict, Any, Tuple, Optional, Union, Set
# from multiprocessing import Pool, cpu_count
# import io
# from src.utils.logger import logger
# from src.utils.config import (
#     HEADER,
#     FMT_HEADER,
#     FMT_LENGTH,
#     FORMAT_TO_STRUCT,
#     STRING_FORMATS,
#     PRECOMPUTED_SCALES,
#     FILE_PATH,
# )
#
#
# class MAVParserProcess:
#     def __init__(self, file_path: str, type_filter: Optional[List[str]] = None):
#         self.file_path = file_path
#         self.fmts: Dict[int, Dict[str, Any]] = {}
#         self.type_filter = set(type_filter) if type_filter else None
#         self.chunks: List[Tuple[int, int]] = []
#
#         self.rounding_columns = {
#             "Lat",
#             "Lng",
#             "TLat",
#             "TLng",
#             "Pitch",
#             "IPE",
#             "Yaw",
#             "IPN",
#             "IYAW",
#             "DesPitch",
#             "NavPitch",
#             "Temp",
#             "AltE",
#             "VDop",
#             "VAcc",
#             "Roll",
#             "HAGL",
#             "SM",
#             "VWN",
#             "VWE",
#             "IVT",
#             "SAcc",
#             "TAW",
#             "IPD",
#             "ErrRP",
#             "SVT",
#             "SP",
#             "TAT",
#             "GZ",
#             "HDop",
#             "NavRoll",
#             "NavBrg",
#             "TAsp",
#             "HAcc",
#             "DesRoll",
#             "SH",
#             "TBrg",
#             "AX",
#         }
#     def scan_file_and_prepare_chunks(self) -> None:
#         self.chunks = []
#         with open(self.file_path, "rb") as f:
#             mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
#             size = len(mm)
#
#             # Scan FMT definitions
#             offset = 0
#             max_head = min(size, 50_000_000)
#             while offset < max_head - 3:
#                 pos = mm.find(FMT_HEADER, offset)
#                 if pos == -1 or pos >= max_head:
#                     break
#                 if pos + FMT_LENGTH <= size:
#                     self._parse_fmt(mm[pos: pos + FMT_LENGTH])
#                     offset = pos + FMT_LENGTH
#                 else:
#                     offset += 1
#
#             # Prepare chunks
#             num_procs = min(cpu_count(), 16)
#             chunk_size = size // num_procs
#             desired_cuts = [chunk_size * (i + 1) for i in range(num_procs - 1)]
#             chunk_start = 0
#
#             for cut in desired_cuts:
#                 offset = max(cut, chunk_start)
#                 while offset < size - 3:
#                     pos = mm.find(HEADER, offset)
#                     if pos == -1:
#                         break
#                     offset = pos
#                     msg_type = mm[offset + 2]
#                     if msg_type in self.fmts:
#                         msg_len = self.fmts[msg_type]["Length"]
#                         if offset + msg_len <= size:
#                             self.chunks.append((chunk_start, offset))
#                             chunk_start = offset
#                             break
#                     offset += 1
#                 if offset >= size - 3:
#                     break
#
#             self.chunks.append((chunk_start, size))
#             mm.close()
#
#     def _parse_fmt(self, chunk: memoryview) -> None:
#         fmt_type, fmt_length, name, fmt_str, cols_raw = struct.unpack_from("<BB4s16s64s", chunk, 3)
#         name = name.rstrip(b"\x00").decode("ascii", errors="ignore")
#         fmt_str = fmt_str.rstrip(b"\x00").decode("ascii", errors="ignore")
#         cols = [c.strip() for c in cols_raw.rstrip(b"\x00").decode("ascii", errors="ignore").split(",") if c.strip()]
#         combined_fmt = "<" + "".join(FORMAT_TO_STRUCT.get(c, "") for c in fmt_str if c in FORMAT_TO_STRUCT)
#         scaling = {col: PRECOMPUTED_SCALES[col] for col in cols if col in PRECOMPUTED_SCALES}
#         rounding = self.rounding_columns.intersection(cols)
#
#
#         self.fmts[fmt_type] = {
#             "Name": name,
#             "Length": fmt_length,
#             "Format": fmt_str,
#             "Columns": cols,
#             "CombinedFmt": combined_fmt,
#             "StructSize": struct.calcsize(combined_fmt),
#             "Scaling": scaling,
#             "Rounding": rounding,
#         }
#
#     @staticmethod
#     def _process_chunk(args) -> Tuple[int, str]:
#         index, file_path, chunk, fmts, type_filter, rounding = args
#         start, end = chunk
#         buffer = io.StringIO()
#         header_written = False
#
#         with open(file_path, "rb") as f:
#             mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
#             mv = memoryview(mm)
#             offset = start
#
#             while offset < end - 3:
#                 if mv[offset] == HEADER[0] and mv[offset + 1] == HEADER[1]:
#                     msg_type = mv[offset + 2]
#                     if msg_type in fmts:
#                         fmt_info = fmts[msg_type]
#                         length = fmt_info["Length"]
#                         if offset + length > end:
#                             break
#                         if type_filter and fmt_info["Name"] not in type_filter:
#                             offset += length
#                             continue
#
#                         msg = MAVParserProcess._parse_message(fmt_info, mv, offset + 3,rounding)
#                         if msg:
#                             if not header_written:
#                                 buffer.write("|".join(msg.keys()) + "\n")
#                                 header_written = True
#                             buffer.write("|".join(str(v) for v in msg.values()) + "\n")
#
#                         offset += length
#                         continue
#                 offset += 1
#
#             del mv
#             mm.close()
#
#         return index, buffer.getvalue()
#
#     @staticmethod
#     def _parse_message(fmt_info: Dict[str, Any], mv: Union[bytes, memoryview], payload_offset: int,rounding: Set[str]) -> Optional[Dict[str, Any]]:
#         message: Dict[str, Any] = {"mavpackettype": fmt_info["Name"]}
#         try:
#             values = struct.unpack_from(fmt_info["CombinedFmt"], mv, payload_offset)
#             for col, val in zip(fmt_info["Columns"], values):
#                 if col in fmt_info["Scaling"]:
#                     val *= fmt_info["Scaling"][col]
#                 if rounding and isinstance(val, float) and col in fmt_info["Rounding"]:
#                     val = round(val, 7)
#                 if fmt_info["Name"] == "GPS" and col == "Alt":
#                     val = round(val, 7)
#                 if isinstance(val, bytes):
#                     val = val.rstrip(b"\x00").decode("ascii", errors="ignore")
#                 message[col] = val
#             return message
#         except Exception:
#             return None
#
#     def run(self, rounding: Optional[Set[str]] = None) -> str:
#         start = time.perf_counter()
#         logger.info("Preparing chunks...")
#         self.scan_file_and_prepare_chunks()
#
#         args_list = [
#             (i, self.file_path, chunk, self.fmts, self.type_filter,rounding)
#             for i, chunk in enumerate(self.chunks)
#         ]
#
#         with Pool(processes=cpu_count()) as pool:
#             results = pool.map(MAVParserProcess._process_chunk, args_list)
#
#         # סדר לפי index כדי לשמור על סדר קבצים
#         results.sort(key=lambda x: x[0])
#
#         # מחברים את כל המחרוזות למשתנה אחד
#         all_messages_buffer = io.StringIO()
#         for _, chunk_content in results:
#             all_messages_buffer.write(chunk_content)
#
#         total_time = time.perf_counter() - start
#         logger.info(f"Parsing finished in {total_time:.2f}s")
#
#         final_content = all_messages_buffer.getvalue()
#         all_messages_buffer.close()
#         return final_content
#
#
# if __name__ == "__main__":
#     # parser = MAVParserProcess(FILE_PATH,type_filter=["GPS"])
#     parser = MAVParserProcess(FILE_PATH)
#     all_messages = parser.run()
#     # from try1 import MAVParserProcess as m
#     # parser = m(FILE_PATH)
#     # all_messages2 = parser.run()
#     # כל ההודעות עכשיו ב-all_messages
#     print(all_messages[:500])  #
#     import csv
#     import io
#
#     # all_messages = המחרוזת שהתקבלה מה-parser
#
#     # יוצרים "קובץ וירטואלי" מ־string
#     f = io.StringIO(all_messages)
#
#     # יוצרים DictReader
#     reader = csv.DictReader(f)
#
#     # שולפים רק את ההודעה הראשונה
#     first_message = next(reader)
#     message = next(reader)
#
#     # מציגים את ההודעה הראשונה במבנה מילון
#     print(first_message)
#     print(message)
#     # print(first_message['Lat'])  # לדוגמה, הערך של Lat


import struct
import mmap
import time
import io
from typing import List, Dict, Any, Tuple, Optional, Union, Set
from multiprocessing import Pool, cpu_count

from src.utils.logger import logger
from src.utils.config import (
    HEADER,
    FMT_HEADER,
    FMT_LENGTH,
    FORMAT_TO_STRUCT,
    ROUNDING,
    PRECOMPUTED_SCALES,
    FILE_PATH,
)


class MAVParserProcess:
    def __init__(self, file_path: str, type_filter: Optional[List[str]] = None):
        self.file_path = file_path
        self.fmts: Dict[int, Dict[str, Any]] = {}
        self.type_filter = set(type_filter) if type_filter else None
        self.chunks: List[Tuple[int, int]] = []

        self.rounding_columns : frozenset = ROUNDING

    def scan_file_and_prepare_chunks(self) -> None:
        self.chunks = []
        with open(self.file_path, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            size = len(mm)

            # Scan FMT definitions
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

            # Prepare chunks
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
    def _process_chunk(args) -> Tuple[int, str]:
        index, file_path, chunk, fmts, type_filter, rounding = args
        start, end = chunk
        buffer = io.StringIO()
        header_written = False

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

                        # parse the message
                        msg = MAVParserProcess._parse_message(fmt_info, mv, offset + 3, rounding)
                        if msg:
                            if not header_written:
                                # כותב את כל המפתחות במילון כ־header
                                buffer.write("|".join(msg.keys()) + "\n")
                                header_written = True

                            # כותב את כל הערכים בסדר של המפתחות
                            row_values = [str(v) for v in msg.values()]
                            buffer.write("|".join(row_values) + "\n")

                        offset += length
                        continue
                offset += 1

            del mv
            mm.close()

        return index, buffer.getvalue()

    @staticmethod
    def _parse_message(fmt_info: Dict[str, Any], mv: Union[bytes, memoryview], payload_offset: int, rounding: Set[str]) -> Optional[Dict[str, Any]]:
        message: Dict[str, Any] = {"mavpackettype": fmt_info["Name"]}
        try:
            values = struct.unpack_from(fmt_info["CombinedFmt"], mv, payload_offset)
            for col, val in zip(fmt_info["Columns"], values):
                if col in fmt_info["Scaling"]:
                    val *= fmt_info["Scaling"][col]
                if rounding and isinstance(val, float) and col in fmt_info["Rounding"]:
                    val = round(val, 7)
                if fmt_info["Name"] == "GPS" and col == "Alt":
                    val = round(val, 7)
                if isinstance(val, bytes):
                    val = val.rstrip(b"\x00").decode("ascii", errors="ignore")
                message[col] = val
            return message
        except Exception:
            return None

    def run(self, rounding: Optional[Set[str]] = None) -> str:
        start = time.perf_counter()
        logger.info("Preparing chunks...")
        self.scan_file_and_prepare_chunks()

        args_list = [
            (i, self.file_path, chunk, self.fmts, self.type_filter, rounding)
            for i, chunk in enumerate(self.chunks)
        ]

        with Pool(processes=cpu_count()) as pool:
            results = pool.map(MAVParserProcess._process_chunk, args_list)

        # סדר לפי index כדי לשמור על סדר קבצים
        results.sort(key=lambda x: x[0])

        # מחברים את כל המחרוזות למשתנה אחד
        all_messages_buffer = io.StringIO()
        for _, chunk_content in results:
            all_messages_buffer.write(chunk_content)

        total_time = time.perf_counter() - start
        logger.info(f"Parsing finished in {total_time:.2f}s")

        final_content = all_messages_buffer.getvalue()
        all_messages_buffer.close()
        return final_content


if __name__ == "__main__":
    import io
    import csv
    import ast
    from pymavlink import mavutil

    # מפענח את הקובץ שלנו
    # parser = MAVParserProcess(FILE_PATH)
    # all_messages = parser.run()
    # print(all_messages[:500])

    # קורא את ה־CSV שכתוב עם | כ־delimiter
    # f = io.StringIO(all_messages)
    # reader = csv.DictReader(f, delimiter="|")

    # פותח חיבור MAVLink
    mav = mavutil.mavlink_connection(FILE_PATH)

    # def smart_cast(value):
    #     """ממיר מחרוזת למספר אם אפשר, אחרת מחזיר את המחרוזת כפי שהיא"""
        # try:
        #     return ast.literal_eval(value)
        # except Exception:
        #     return value

    i = 0
    while True:
        msg = mav.recv_match(blocking=False,type=["FILE"])
        if msg is None:
            break
        msg_dict = msg.to_dict()

        # my_message = next(reader)
        # המר את כל הערכים של CSV לסוגים "חכמים"
        # my_message_casted = {k: smart_cast(v) for k, v in my_message.items()}

        # השוואה

        if "Data" in msg_dict:
            print(msg_dict)
        # if msg_dict != my_message_casted:
        #     print(f"Mismatch at index: {i}")
        #     print("MAVLink message:", msg_dict)
        #     print("CSV message   :", my_message_casted)
        #     break
        i += 1

    print(f"Checked {i} messages successfully. No mismatches found.")


