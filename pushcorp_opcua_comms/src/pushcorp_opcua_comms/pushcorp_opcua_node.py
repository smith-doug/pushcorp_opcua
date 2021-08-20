#!/usr/bin/env python3

import asyncio
import traceback
from enum import IntEnum
from typing import Dict, Optional, List

import aiorospy
import asyncua.common.ua_utils as ua_utils
import asyncua.ua
import pushcorp_msgs
import pushcorp_msgs.srv
import rospy
import std_srvs.srv
from asyncua import Client, ua, Node
from asyncua.common.subscription import Subscription
from codetiming import Timer
from std_msgs.msg import Float32, Bool, Int32


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
        try:
            self.data_type = await self.node.read_data_type_as_variant_type()
            self.value = await self.node.read_value()
            return True
        #except asyncua.ua.UaError.BadAttributeIdInvalid:

        except ua.UaStatusCodeError as er:
            if er.code not in [ua.StatusCodes.BadAttributeIdInvalid, ua.StatusCodes.BadNotImplemented]:
                rospy.logerr(f'Unexpected error in NodeData.init: {er}')
            else:
                pass
            return False

    def get_value(self):
        return self.value


async def create_node_data(client: Client, nodeid: str, node: Node = None) -> Optional[NodeData]:
    nd = NodeData(client, nodeid, node)
    ok = await nd.init()
    if ok:
        return nd
    else:
        return None


class OpcuaData:

    # noinspection PyTypeChecker
    def __init__(self, client: Client, params: OpcuaDataParams):

        self.params = params
        self.ns = params.ns
        self.nodeid = params.nodeid
        self.event_loop = asyncio.get_running_loop()
        self.client: Client = client

        self.heartbeat: Node = None  # Heartbeat gets written separately

        # self.st_data = None  # Full data (hb, input, output), will be a struct created by magic
        # self.st_data_input = None

        self.st_root_node: Node = None  # Node the the full data
        self.st_data_input_node: Node = None  # Node to the input (writable) data
        self.sub_opcua: Subscription = None
        self.lock = asyncio.Lock()

        self.subs = {}  # Ros subscribers
        self.pubs = {}  # Ros publishers
        self.srvs = []  # Ros services

        self.status_nodes: Dict[str, NodeData] = {}  # Status values written/read from the PLC
        self.input_nodes: Dict[str, NodeData] = {}  # Values written to the PLC
        self.output_nodes: Dict[str, NodeData] = {}  # Values read from the PLC
        self.all_nodes: Dict[str, NodeData] = {}  #

        self.updated = asyncio.Event()  # Set when st_data is updated by event

    # async def map_data_input(self, base_node: Node):
    #
    #     input_nodeid = base_node.nodeid.to_string() + '.input'
    #     input_node = self.client.get_node(input_nodeid)
    #     child_nodes = await ua_utils.get_node_children(input_node)
    #
    #     node: Node
    #     for node in child_nodes[1:]:
    #         var_name = str(node.nodeid.Identifier).replace(input_node.nodeid.Identifier + '.', '')
    #         nd = await create_node_data(self.client, '', node=node)
    #         self.input_nodes[var_name] = nd

    async def map_children(self, base_node: Node, subnode_str: str) -> Dict[str, NodeData]:
        sub_nodeid = f'{base_node.nodeid.to_string()}.{subnode_str}'
        sub_node = self.client.get_node(sub_nodeid)



        child_nodes = await ua_utils.get_node_children(sub_node)

        ret_nodes: Dict[str, NodeData] = {}
        node: Node
        for node in child_nodes[1:]:
            var_name = f'{subnode_str}.' + str(node.nodeid.Identifier).replace(sub_node.nodeid.Identifier + '.', '')
            nd = await create_node_data(self.client, '', node=node)
            if nd is not None:
                ret_nodes[var_name] = nd

        return ret_nodes

    async def map_io_data_nodes(self):
        self.status_nodes = await self.map_children(self.st_root_node, 'status')
        self.input_nodes = await self.map_children(self.st_root_node, 'input')
        self.output_nodes = await self.map_children(self.st_root_node, 'output')

        self.all_nodes = {**self.status_nodes, **self.input_nodes, **self.output_nodes}

        self.heartbeat = self.status_nodes['status.input.Heartbeat']


    def make_publisher(self, node: Node) -> rospy.Publisher:
        pass

    async def map_data(self):
        rospy.loginfo(f'OpcuaData mapping with nodeid {self.nodeid}')

        self.st_root_node = self.client.get_node(self.nodeid)

        await self.map_io_data_nodes()

        # Subscribe to the entire struct
        self.sub_opcua = await self.client.create_subscription(self.params.opcua_sub_pd, self)
        # await self.sub_opcua.subscribe_data_change(self.st_data_node)

        items_status = [node.node for node in list(self.status_nodes.values())]
        items_in = [node.node for node in list(self.input_nodes.values())]
        items_out = [node.node for node in list(self.output_nodes.values())]

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
            else:
                rospy.logerr(f'Unknown type {key}, {type(val)} in structure')

            if pub is not None:
                self.pubs[str(key)] = pub

        self.event_loop.create_task(self.topic_publishers_init())

        # Create services
        for key, nd in self.input_nodes.items():
            srv = None

            name = str(key)
            topic_name = self.ns + '/' + name.replace('.', '/')
            val = nd.value
            if isinstance(val, float):
                srv = aiorospy.AsyncService(topic_name, pushcorp_msgs.srv.SetFloat32,
                                            (lambda req, var_name=name: self.svc_handler(req, var_name,
                                                                                     pushcorp_msgs.srv.SetFloat32Response)))
            elif isinstance(val, bool):
                srv = aiorospy.AsyncService(topic_name, std_srvs.srv.SetBool,
                                            (lambda req, var_name=name: self.svc_handler(req, var_name,
                                                                                     std_srvs.srv.SetBoolResponse)))
            elif isinstance(val, int) or isinstance(val, IntEnum):
                srv = aiorospy.AsyncService(topic_name, pushcorp_msgs.srv.SetInt32,
                                            (lambda req, var_name=name: self.svc_handler(req, var_name,
                                                                                     pushcorp_msgs.srv.SetInt32Response)))
            else:
                rospy.logerr(f'Unknown type {key}, {type(val)} in structure')

            if srv is not None:
                self.srvs.append(srv)

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
        except:
            traceback.print_exc()
            raise

    async def topic_publishers_init(self):
        for key, val in self.pubs.items():
            self.event_loop.create_task(self.topic_publisher(key, val))

    async def topic_listener(self, name, sub):
        async for msg in sub.subscribe():
            print(f'{sub.name} Heard {msg.data}')
            await self.set_named_val(name, msg.data)

    async def topic_listeners_init(self):
        await asyncio.gather(*[self.topic_listener(sub[0], sub[1]) for sub in self.subs.items()])

    async def datachange_notification(self, node, val, data):
        nd = next(nd for nd in self.all_nodes.values() if nd.node == node)
        nd.value = val

    async def heartbeat_co(self):
        hb_nd: NodeData = self.status_nodes['status.input.Heartbeat']
        while True:
            val = not hb_nd.value
            try:
                await self.set_named_val('status.input.Heartbeat', val)
                #await hb_nd.node.write_value(ua.DataValue(val, hb_nd.data_type))
            except asyncio.exceptions.TimeoutError:
                rospy.logerr(f"{self.ns} Heartbeat to opcua timed out")
                raise
            except:
                traceback.print_exc()
            await asyncio.sleep(1)

    async def set_named_val(self, name: str, value: any):

        nd: NodeData = self.all_nodes[name]
        try:
            await nd.node.write_value(ua.DataValue(ua.Variant(Value=value, VariantType=nd.data_type)))
        except:
            traceback.print_exc()


class OpcuaComms:

    # noinspection PyTypeChecker
    def __init__(self, opcua_endpoint: str, opcua_params: list):
        self.opcua_endpoint = opcua_endpoint
        self.opcua_params = opcua_params

        self.event_loop = None
        self.client: Client = None

        self.opcua_data_list: list[OpcuaData] = []

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


if __name__ == '__main__':
    rospy.init_node('pushcorp_comms_node', anonymous=False)

    loop = asyncio.get_event_loop()
    loop.set_debug(False)

    opcua_ep = rospy.get_param('~opcua_endpoint', "opc.tcp://192.168.125.39:4840")

    param_list = []
    fcu_params = OpcuaDataParams(nodeid='ns=4;s=GVL_opcua.stFcuControl', ns='/pushcorp/fcu')
    spindle_params = OpcuaDataParams(nodeid='ns=4;s=GVL_opcua.stSpindleControl', ns='/pushcorp/spindle')

    param_list.append(fcu_params)
    param_list.append(spindle_params)

    test_basic_params = OpcuaDataParams(nodeid='ns=4;s=GVL_opcua.stTestBasic', ns='/pushcorp/test_basic')

    # pc = OpcuaComms(opcua_ep, param_list)
    pc = OpcuaComms(opcua_ep, [*param_list, test_basic_params])

    #task = loop.create_task(pc.run())

    test_basic_params_keba = OpcuaDataParams(nodeid='ns=4;s=APPL.Application.GVL_opcua.stTestBasic', ns='/pushcorp/test_basic')
    #pc2 = OpcuaComms('opc.tcp://192.168.72.3:4842', [test_basic_params_keba])

    task = loop.create_task(pc.run())
    # aiorospy.cancel_on_exception(task)
    aiorospy.cancel_on_shutdown(task)

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    except:
        traceback.print_exc()
