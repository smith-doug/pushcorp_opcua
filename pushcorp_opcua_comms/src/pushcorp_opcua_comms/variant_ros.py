from typing import Optional, Type

import std_msgs
from asyncua import Client, ua, Node
import pushcorp_msgs.srv as opcsrv
import pushcorp_msgs.msg as opcmsg
import std_srvs.srv
import std_msgs.msg
from enum import IntEnum


def variant_to_msg(variant: ua.Variant) -> Optional[Type]:
    msg_type: Optional[Type] = None

    val = variant.Value
    if variant.is_array and len(variant.Value) > 0:
        val = variant.Value[0]

    if isinstance(val, float):
        msg_type = std_msgs.msg.Float32
    elif isinstance(val, bool):
        msg_type = std_msgs.msg.Bool
    elif isinstance(val, int) or isinstance(val, IntEnum):
        msg_type = std_msgs.msg.Int32
        if variant.is_array:
            msg_type = opcmsg.Int32Array
    elif isinstance(val, str):
        msg_type = std_msgs.msg.String
    else:
        raise TypeError(f'unknown type {type(val)} in structure')

    return msg_type
