import time

from src.business_logic.mav_parser_linear import MAVParserLinear
from src.business_logic.mav_parser_process import MAVParserProcess
from src.business_logic.mav_parser_threads import MAVParserThreads
from pymavlink import mavutil

class ParserRunners:
    def __init__(self, file_path):
        self.file_path = file_path
        self.runners = {
            "pymavlink": self.run_mavutil,
            "linear": self.run_linear,
            "process": self.run_process,
            "threads": self.run_threads
        }

    def run_mavutil(self, save=True, type_filter=None):
        start = time.perf_counter()
        mav = mavutil.mavlink_connection(self.file_path)
        msgs = []
        while True:
            if type_filter:
                msg = mav.recv_match(blocking=False, type=type_filter)
            else:
                msg = mav.recv_match(blocking=False)
            if msg is None:
                break
            if save:
                msgs.append(msg)
        end = time.perf_counter()
        return "pymavlink", round(end - start, 3), save

    def run_linear(self, save=True, type_filter=None):
        start = time.perf_counter()
        with MAVParserLinear(self.file_path, type_filter=type_filter) as parser:
            if save:
                parser.parse_all()
            else:
                while _ := parser.parse_next():
                    pass
        end = time.perf_counter()
        return "one run", round(end - start, 3), save

    def run_process(self, save=True, type_filter=None):
        start = time.perf_counter()
        process = MAVParserProcess(self.file_path, type_filter=type_filter)
        process.run()
        end = time.perf_counter()
        return "process", round(end - start, 3), save

    def run_threads(self, save=True, type_filter=None):
        start = time.perf_counter()
        threads = MAVParserThreads(self.file_path, type_filter=type_filter)
        threads.run()
        end = time.perf_counter()
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