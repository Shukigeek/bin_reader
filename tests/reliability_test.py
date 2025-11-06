from pymavlink import mavutil
from src.utils.config import FILE_PATH
from typing import Optional,List,Dict,Any
from src.business_logic.mav_parser_linear import MAVParserLinear
from src.business_logic.mav_parser_process import MAVParserProcess
from src.business_logic.mav_parser_threads import MAVParserThreads



def read_pymavlink(file_path: str =FILE_PATH, limit : Optional[int]= None,type_filter:Optional[List[str]] = None) -> List[Dict[str,Any]]:
    mav = mavutil.mavlink_connection(file_path)
    messages = []
    i = 0
    if type_filter:
        while msg := mav.recv_match(blocking=False,type=type_filter):
            messages.append(msg.to_dict())
            if limit:
                i += 1
                if i > limit:
                    break
    else:
        while msg := mav.recv_match(blocking=False):
            messages.append(msg.to_dict())
            if limit:
                i += 1
                if i > limit:
                    break
    return messages

def read_linear(file_path:str = FILE_PATH,limit : Optional[int]=None,type_filter:Optional[List[str]] = None) -> List[Dict[str,Any]]:

    with MAVParserLinear(file_path,type_filter=type_filter) as parser:
        if limit:
            all_msgs = []
            for i in range(limit):
                all_msgs.append(parser.parse_next())
        else:
            all_msgs = parser.parse_all()

    return all_msgs

def read_process(file_path:str = FILE_PATH,limit : Optional[int]=None,type_filter:Optional[List[str]] = None) -> List[Dict[str,Any]]:
    parser = MAVParserProcess(file_path, type_filter=type_filter)
    parser.run()
    if limit:
        return parser.messages[:limit+1]
    else:
        return parser.messages

def read_threads(file_path:str = FILE_PATH,limit : Optional[int]=None,type_filter:Optional[List[str]] = None) -> List[Dict[str,Any]]:
    parser = MAVParserThreads(file_path,type_filter=type_filter)
    parser.run()
    if limit:
        return parser.messages[:limit+1]
    else:
        return parser.messages


if __name__ == '__main__':


    choose_limit = input("enter number or press Enter to check all messages")
    choose_type = input("enter type or press Enter to check all the typs")
    limit = None
    if choose_limit.isdigit():
        limit = int(choose_limit)

    mavlink = read_pymavlink(limit=limit,type_filter=[choose_type])
    all_parser = [read_linear,read_threads,read_process]
    name = {0:"linear",1:"threads",2:"process"}
    for j in range(len(all_parser)):
        my_parser = all_parser[j](limit=limit,type_filter=[choose_type])
        for i in range(len(mavlink)):
            if mavlink[i] != my_parser[i] and "Default" not in mavlink[i]:
                print(i)
                print(mavlink[i])
                print(my_parser[i])
                print(name[j])
                break
    print('end')




