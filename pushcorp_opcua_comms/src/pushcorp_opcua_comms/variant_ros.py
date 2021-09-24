from typing import Optional, Type

from asyncua import Client, ua, Node


def variant_to_msg(variant: ua.Variant) -> Optional[Type]:
