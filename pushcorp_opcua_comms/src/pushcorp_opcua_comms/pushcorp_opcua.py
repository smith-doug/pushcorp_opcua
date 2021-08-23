#!/usr/bin/env python3

import asyncio
import traceback
from enum import IntEnum
from typing import Dict, Optional, List, Any

import asyncua.common.ua_utils as ua_utils
import pushcorp_msgs.srv
import rospy
import std_srvs.srv
from asyncua import Client, ua, Node
from asyncua.common.subscription import Subscription
#  from codetiming import Timer
from std_msgs.msg import Float32, Bool, Int32, String

import aiorospy


class OpcuaDataParams:
    def __init__(self, nodeid='', ns='', topic_pub_pd=0.01, opcua_sub_pd=0.005):
        self.nodeid = nodeid
        self.ns = ns
        self.topic_pub_pd = topic_pub_pd
        self.opcua_sub_pd = opcua_sub_pd


class NodeData:
    # noinspection PyTypeChecker
    def __init__(self, client: Client, nodeid: str, node: Node = None):
        self.client = client
        self.data_type: ua.VariantType = None
        self.value = any

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
            return True

        # @todo find a better way to determine if the node is a variable with a value or not.
        #  Beckhoff returns BadAttributeIdInvalid, Keba returns BadNotImplemented for the struct element
        except ua.UaStatusCodeError as er:
            if er.code not in [ua.StatusCodes.BadAttributeIdInvalid, ua.StatusCodes.BadNotImplemented]:
                rospy.logerr(f'Unexpected error in NodeData.init: {er}')
            else:
                pass
            return False

    def get_value(self):
        return self.value


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


class OpcuaData:
    """
    Creates and manages topics and services for specially defined opcua data structures.

    The mapped data must have the following structure:
    {
        status {
            input {
                Heartbeat: BOOL;
            }
            output {
                Heartbeat	: BOOL;
                Error	: BOOL;
                Connected	: BOOL;
            }
        }
        input {
            data from ros->plc
        }
        output {
            data from plc->ros
        }
    }
    """

    # noinspection PyTypeChecker
    def __init__(self, client: Client, params: OpcuaDataParams):

        self.params = params
        self.ns = params.ns
        self.nodeid = params.nodeid
        self.event_loop = asyncio.get_running_loop()
        self.client: Client = client

        self.st_root_node: Node = None  # Node the the full data
        self.st_data_input_node: Node = None  # Node to the input (writable) data
        self.sub_opcua: Subscription = None

        self.pubs = {}  # Ros publishers
        self.srvs = []  # Ros services

        # Status values written/read from the PLC
        self.status_nodes: Dict[str, NodeData] = {}
        self.input_nodes: Dict[str, NodeData] = {}  # Values written to the PLC
        self.output_nodes: Dict[str, NodeData] = {}  # Values read from the PLC

        # Full dict of all nodes.  Stored as {important part of node name, nodedata}
        self.all_nodes: Dict[str, NodeData] = {}

        self.updated = asyncio.Event()  # Set when st_data is updated by event

    async def map_children(self, base_node: Node, subnode_str: str) -> Dict[str, NodeData]:
        sub_nodeid = f'{base_node.nodeid.to_string()}.{subnode_str}'
        sub_node = self.client.get_node(sub_nodeid)

        child_nodes = await ua_utils.get_node_children(sub_node)

        ret_nodes: Dict[str, NodeData] = {}
        node: Node
        for node in child_nodes[1:]:
            var_name = node.nodeid.Identifier.replace(base_node.nodeid.Identifier + '.', '')
            nd = await create_node_data(self.client, '', node=node)
            if nd is not None:
                ret_nodes[var_name] = nd

        return ret_nodes

    async def map_io_data_nodes(self):
        self.status_nodes = await self.map_children(self.st_root_node, 'status')
        self.input_nodes = await self.map_children(self.st_root_node, 'input')
        self.output_nodes = await self.map_children(self.st_root_node, 'output')

        self.all_nodes = {**self.status_nodes, **self.input_nodes, **self.output_nodes}

    def make_publisher(self, node: Node) -> rospy.Publisher:
        pass

    async def map_data(self):
        rospy.loginfo(f'OpcuaData mapping with nodeid {self.nodeid}')

        self.st_root_node = self.client.get_node(self.nodeid)
        await self.map_io_data_nodes()

        # subscribe to all the data nodes
        self.sub_opcua = await self.client.create_subscription(self.params.opcua_sub_pd, self)

        items_status = [nd.node for nd in list(self.status_nodes.values())]
        items_in = [nd.node for nd in list(self.input_nodes.values())]
        items_out = [nd.node for nd in list(self.output_nodes.values())]

        await self.sub_opcua.subscribe_data_change([*items_status, *items_in, *items_out])

        # Create topic publishers for everything
        for key, nd in self.all_nodes.items():
            pub = None
            topic_name = self.ns + '/' + str(key)
            topic_name = topic_name.replace('.', '/')
            val = nd.value
            if isinstance(val, float):
                pub = rospy.Publisher(topic_name, Float32, queue_size=1)
            elif isinstance(val, bool):
                pub = rospy.Publisher(topic_name, Bool, queue_size=1)
            elif isinstance(val, int) or isinstance(val, IntEnum):
                pub = rospy.Publisher(topic_name, Int32, queue_size=1)
            elif isinstance(val, str):
                pub = rospy.Publisher(topic_name, String, queue_size=1)
            else:
                rospy.logerr(f'Unknown type {key}, {type(val)} in structure')

            if pub is not None:
                self.pubs[str(key)] = pub

        self.event_loop.create_task(self.topic_publishers_init())

        # Create services.  The callbacks will be lambdas to store the name of the var to update so that 1 handler
        # can be used for all the dynamically created services
        for key, nd in self.input_nodes.items():
            srv = None

            name = str(key)  # store this for the service call
            topic_name = self.ns + '/' + name.replace('.', '/')  # ex: ns/status/input/Heartbeat
            val = nd.value

            srv_type = None

            if isinstance(val, float):
                srv_type = pushcorp_msgs.srv.SetFloat32
            elif isinstance(val, bool):
                srv_type = std_srvs.srv.SetBool
            elif isinstance(val, int) or isinstance(val, IntEnum):
                srv_type = pushcorp_msgs.srv.SetInt32
            elif isinstance(val, str):
                srv_type = pushcorp_msgs.srv.SetString
            else:
                rospy.logerr(f'Unknown type {key}, {type(val)} in structure')

            if srv_type is not None:
                srv_resp_type = srv_type._response_class
                srv = aiorospy.AsyncService(topic_name, srv_type,
                                            (lambda req, var_name=name: self.svc_handler(req, var_name, srv_resp_type)))

            if srv is not None:
                self.srvs.append(srv)

        # Create handlers in a separate tasks so I can freely await and not worry about blocking
        self.event_loop.create_task(self.heartbeat_co())
        self.event_loop.create_task(self.svc_handlers_init())
        self.event_loop.create_task(self.topic_publishers_init())

    async def svc_handler(self, req, name, ret_type):
        name = name
        rospy.logdebug('Updating %s to %s', name, req.data)
        try:
            await self.set_named_val(name, req.data)
            return ret_type(success=True)
        except asyncio.exceptions:
            return ret_type(success=False, message=traceback.format_exc())

    async def svc_handlers_init(self):
        await asyncio.gather(*[srv.start() for srv in self.srvs])

    async def topic_publisher(self, name, pub: rospy.Publisher):
        try:
            nd = self.all_nodes[name]
            while True:
                if pub.get_num_connections() > 0:
                    val = nd.value
                    pub.publish(val)
                else:
                    await(asyncio.sleep(1.0))

                await asyncio.sleep(self.params.topic_pub_pd)
        except BaseException:
            traceback.print_exc()
            raise

    async def topic_publishers_init(self):
        for key, val in self.pubs.items():
            self.event_loop.create_task(self.topic_publisher(key, val))

    async def datachange_notification(self, node, val, data):
        """
        Callback for subscribed opcua data
        :param node:
        :param val:
        :param data:
        """
        # Find the correct NodeData and update its value
        nd = next(nd for nd in self.all_nodes.values() if nd.node == node)
        nd.value = val

    async def heartbeat_co(self):
        hb_nd: NodeData = self.status_nodes['status.input.Heartbeat']
        while True:
            val = not hb_nd.value
            try:
                await self.set_named_val('status.input.Heartbeat', val)
            except asyncio.exceptions.TimeoutError:
                rospy.logerr(f"{self.ns} Heartbeat to opcua timed out")
                raise
            except BaseException:
                traceback.print_exc()
            await asyncio.sleep(1)

    async def set_named_val(self, name: str, value: Any):

        nd: NodeData = self.all_nodes[name]
        try:
            await nd.node.write_value(ua.DataValue(ua.Variant(Value=value, VariantType=nd.data_type)))
        except BaseException:
            traceback.print_exc()
            raise


class OpcuaComms:

    # noinspection PyTypeChecker
    def __init__(self, opcua_endpoint: str, opcua_params: list):
        self.opcua_endpoint = opcua_endpoint
        self.opcua_params = opcua_params

        self.event_loop = None
        self.client: Client = None

        self.opcua_data_list: List[OpcuaData] = []

    # Simple way to keep the loop alive
    async def keep_alive(self):
        while True:
            await asyncio.sleep(1.0)

    async def connect(self):
        self.event_loop = asyncio.get_running_loop()
        self.client = Client(self.opcua_endpoint)
        try:
            await self.client.connect()
            rospy.loginfo(f'opcua client connected to {self.opcua_endpoint}')

            # Load types so they're available to the data objects
            await self.client.load_data_type_definitions()

            for param in self.opcua_params:
                self.opcua_data_list.append(OpcuaData(self.client, param))
                rospy.loginfo(f'Created OpcuaData {param.nodeid}, {param.ns}')

            await asyncio.gather(*[data.map_data() for data in self.opcua_data_list],
                                 self.keep_alive())

        except asyncio.CancelledError:
            raise
        except:
            traceback.print_exc()
        finally:
            pass

    async def run(self):
        await self.connect()

    async def disconnect(self):
        await self.client.disconnect()
