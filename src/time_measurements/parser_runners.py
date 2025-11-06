import time

from src.business_logic.mav_parser_linear import MAVParserLinear
from src.business_logic.mav_parser_process import MAVParserProcess
from src.business_logic.mav_parser_threads import MAVParserThreads
from pymavlink import mavutil
import os
import shutil

TEMP_DIR = "src/tmp"

if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)




class ParserRunners:
    def __init__(self, file_path):
        self.file_path = file_path
        self.runners = {
            "pymavlink": self.run_mavutil,
            "linear": self.run_linear,
            "process": self.run_process,
            "threads": self.run_threads
        }

    def clean_temp(self):
        """Clean only temporary files/folders created by parsers."""
        temp_dirs = ["src/tmp", "/tmp/mav_chunks"]
        for d in temp_dirs:
            if os.path.exists(d):
                for item in os.listdir(d):
                    item_path = os.path.join(d, item)
                    try:
                        if os.path.isfile(item_path) or os.path.islink(item_path):
                            os.unlink(item_path)
                        elif os.path.isdir(item_path):
                            shutil.rmtree(item_path)
                    except Exception as e:
                        print(f"Warning: failed to delete {item_path}: {e}")
    def run_mavutil(self, save=True, type_filter=None):
        start = time.perf_counter()
        mav = mavutil.mavlink_connection(self.file_path)
        msgs = []
        while msg := mav.recv_match(blocking=False, type=type_filter):
            if save:
                msgs.append(msg)
        end = time.perf_counter()
        print(len(msgs))
        del msgs
        self.clean_temp()
        return "pymavlink", round(end - start, 3), save

    def run_linear(self, save=True, type_filter=None):
        start = time.perf_counter()
        msgs = []
        with MAVParserLinear(self.file_path, type_filter=type_filter) as parser:
            if save:
                msgs = parser.parse_all()
            else:
                while _ := parser.parse_next():
                    pass
        end = time.perf_counter()
        print(len(msgs))
        del msgs
        self.clean_temp()
        return "linear", round(end - start, 3), save

    def run_process(self, save=True, type_filter=None):
        start = time.perf_counter()
        process = MAVParserProcess(self.file_path, type_filter=type_filter)
        msgs = process.run()
        end = time.perf_counter()
        print(len(msgs))
        del msgs
        self.clean_temp()
        return "process", round(end - start, 3), save

    def run_threads(self, save=True, type_filter=None):
        start = time.perf_counter()
        threads = MAVParserThreads(self.file_path, type_filter=type_filter)
        msgs = threads.run()
        end = time.perf_counter()
        print(len(msgs))
        del msgs
        self.clean_temp()
        return "threads", round(end - start, 3), save

    def run_all(self, selected=None, category="all messages", save_list=True, type_filter=None):
        selected = selected or list(self.runners.keys())
        data = []

        for name in selected:
            func = self.runners[name]

            if name in ["pymavlink", "linear"]:
                for save_val in [False, True]:
                    lib_name, elapsed, saved = func(save=save_val, type_filter=type_filter)
                    data.append({
                        "category": category,
                        "library": lib_name,
                        "save": saved,
                        "time": elapsed
                    })
            else:
                lib_name, elapsed, saved = func(save=save_list, type_filter=type_filter)
                data.append({
                    "category": category,
                    "library": lib_name,
                    "save": saved,
                    "time": elapsed
                })

        return data