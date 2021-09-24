from typing import Optional

import rospy
from asyncua import Client, Node, ua


class NodeData:
    # noinspection PyTypeChecker
    def __init__(self, client: Client, nodeid: str, node: Node = None):
        self.client = client
        self.data_type: ua.VariantType = None
        self.value = any

        self.variant: ua.Variant = None

        if node is None:
            self.nodeid = nodeid
            self.node = client.get_node(nodeid)
        else:
            self.node = node
            self.nodeid = node.nodeid.to_string()

    async def init(self) -> bool:
        """
        Initialize this NodeData.  This needs to await to get the data from the server.
        :return: True if it was possible to get the node data type and value
        """
        try:
            self.data_type = await self.node.read_data_type_as_variant_type()
            self.value = await self.node.read_value()  # Read the value now to get its type
            self.variant = (await self.node.read_data_value()).Value
            return True

        # @todo find a better way to determine if the node is a variable with a value or not.
        except ua.UaStatusCodeError as er:
            #  Beckhoff returns BadAttributeIdInvalid, Keba returns BadNotImplemented for the struct element.
            #  ok to continue
            if er.code in [ua.StatusCodes.BadAttributeIdInvalid, ua.StatusCodes.BadNotImplemented]:
                pass
            else:
                rospy.logerr(f'Unexpected error in NodeData.init: {er}')
            return False

    def get_value(self):
        return self.value

    def __eq__(self, other):
        if isinstance(other, Node):
            return self.node == other
        else:
            return isinstance(other, NodeData) and self.node == other.node


async def create_node_data(client: Client, nodeid: str, node: Node = None) -> Optional[NodeData]:
    """
    Create and init a new NodeData
    :param client:
    :param nodeid:
    :param node:
    :return: Initialized NodeData or None if it failed
    """
    nd = NodeData(client, nodeid, node)
    ok = await nd.init()
    if ok:
        return nd
    else:
        return None
