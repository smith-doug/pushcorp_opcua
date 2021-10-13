#!/usr/bin/env python3

import asyncio
import traceback
from enum import IntEnum
from typing import Dict, List, Any

import asyncua.common.ua_utils as ua_utils
import pushcorp_msgs.srv
import pushcorp_msgs.msg as pcm

import rospy
import std_srvs.srv
from asyncua.ua import UaStatusCodeError
from asyncua import Client, ua, Node
from asyncua.common.subscription import Subscription
from std_msgs.msg import Float32
import std_msgs.msg

import aiorospy
from pushcorp_opcua_comms.nodedata import NodeData, create_node_data


class OpcuaDataParams:
    def __init__(self, nodeid='', ns='', topic_pub_pd=10, opcua_sub_pd=10):
        """
        Parameters for this OpcuaData instance
        :param nodeid: String to the struct, like 'ns=4;s=APPL.Application.GVL_opcua.stTestBasic'
        :param ns: Namespace to create topics/services in
        :param topic_pub_pd: Period for publishing topics in ms
        :param opcua_sub_pd: Desired period for opcua subscription.  May be overridden by the server
        """
        self.nodeid = nodeid
        self.ns = ns
        self.topic_pub_pd = topic_pub_pd / 1000.0
        self.opcua_sub_pd = opcua_sub_pd


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
                Error	: UINT;
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

    async def map_children(self, root_node: Node, subnode_str: str) -> Dict[str, NodeData]:
        f"""
        Creates a NodeData for each subnode of (root_node.nodeid).(subnode_str) 
        :param root_node: The root node of the structure to be mapped
        :param subnode_str:
        :return:
        """
        sub_nodeid = f'{root_node.nodeid.to_string()}.{subnode_str}'
        sub_node = self.client.get_node(sub_nodeid)

        child_nodes = await ua_utils.get_node_children(sub_node)

        ret_nodes: Dict[str, NodeData] = {}
        node: Node
        for node in child_nodes[1:]:
            var_name = node.nodeid.Identifier.replace(root_node.nodeid.Identifier + '.', '')
            nd = await create_node_data(self.client, '', node=node)
            if nd is not None:
                ret_nodes[var_name] = nd

        return ret_nodes

    async def map_io_data_nodes(self):
        self.status_nodes = await self.map_children(self.st_root_node, 'status')
        self.input_nodes = await self.map_children(self.st_root_node, 'input')
        self.output_nodes = await self.map_children(self.st_root_node, 'output')

        self.all_nodes = {**self.status_nodes, **self.input_nodes, **self.output_nodes}

    async def map_data(self):
        rospy.loginfo(f'OpcuaData mapping with nodeid {self.nodeid}')

        self.st_root_node = self.client.get_node(self.nodeid)
        await self.map_io_data_nodes()

        # subscribe to all the data nodes
        self.sub_opcua = await self.client.create_subscription(self.params.opcua_sub_pd, self)
        rospy.loginfo(
            f'Requested subscription with period {self.params.opcua_sub_pd}, actual period is {self.sub_opcua.parameters.RequestedPublishingInterval}')

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
            msg_type = None

            # @TODO Maybe just check if there is a [ in the key name
            variant = nd.variant
            if variant.is_array:
                val = variant.Value[0]

            if isinstance(val, float):
                msg_type = std_msgs.msg.Float32
            elif isinstance(val, bool):
                msg_type = std_msgs.msg.Bool
            elif isinstance(val, int) or isinstance(val, IntEnum):
                msg_type = std_msgs.msg.Int32
                if variant.is_array:
                    msg_type = pcm.Int32Array
            elif isinstance(val, str):
                msg_type = std_msgs.msg.String
            else:
                rospy.logerr(f'Unknown type {key}, {type(val)} in structure')

            if msg_type is not None:
                try:
                    pub = rospy.Publisher(topic_name, msg_type, queue_size=1)
                    self.pubs[str(key)] = pub
                except:
                    print('')

            blah = std_msgs.msg.Bool

        self.event_loop.create_task(self.topic_publishers_init())

        # Create services.  The callbacks will be lambdas to store the name of the var to update so that 1 handler
        # can be used for all the dynamically created services
        for key, nd in self.input_nodes.items():
            srv = None

            name = str(key)  # store this for the service call
            topic_name = self.ns + '/' + name.replace('.', '/')  # ex: ns/status/input/Heartbeat
            val = nd.value

            variant = nd.variant
            if variant.is_array:
                val = variant.Value[0]

            srv_type = None

            if isinstance(val, float):
                srv_type = pushcorp_msgs.srv.SetFloat32
            elif isinstance(val, bool):
                srv_type = std_srvs.srv.SetBool
            elif isinstance(val, int) or isinstance(val, IntEnum):
                srv_type = pushcorp_msgs.srv.SetInt32
                if variant.is_array:
                    srv_type = pushcorp_msgs.srv.SetInt32Array
            elif isinstance(val, str):
                srv_type = pushcorp_msgs.srv.SetString
            else:
                rospy.logerr(f'Unknown type {key}, {type(val)} in structure')

            if srv_type is not None:
                srv_resp_type = srv_type._response_class
                srv = aiorospy.AsyncService(topic_name, srv_type,
                                            (lambda req, var_name=name, ret_type=srv_resp_type:
                                             self.svc_handler(req, var_name, ret_type)))

            if srv is not None:
                self.srvs.append(srv)

        # Create handlers in a separate tasks so I can freely await and not worry about blocking
        self.event_loop.create_task(self.heartbeat_co())
        self.event_loop.create_task(self.svc_handlers_init())
        self.event_loop.create_task(self.topic_publishers_init())

    async def svc_handler(self, req, name, ret_type):
        name = name
        rospy.loginfo('Updating %s to %s', name, req.data)

        try:
            await self.set_named_val(name, req.data)
            return ret_type(success=True, message='')
        except ValueError as ex:
            return ret_type(success=False, message=str(ex))
        except:
            traceback.print_exc()
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
                    await asyncio.sleep(self.params.topic_pub_pd)
                else:
                    await(asyncio.sleep(1.0))

        except BaseException:
            traceback.print_exc()
            raise

    async def topic_publishers_init(self):
        for key, val in self.pubs.items():
            self.event_loop.create_task(self.topic_publisher(key, val))

    async def datachange_notification(self, node: Node, val, data):
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
            except UaStatusCodeError as ex:
                rospy.logerr(str(ex))
                raise
            except BaseException:
                traceback.print_exc()
            await asyncio.sleep(1)

    async def set_named_val(self, name: str, value: Any):
        nd: NodeData = self.all_nodes[name]

        try:
            variant = nd.variant
            val = value
            if variant.is_array:
                val = list(val)
                len_old = len(variant.Value)
                len_new = len(val)
                if len(val) != len(variant.Value):
                    raise ValueError(f'Writing array failed.  Expected len: {len(variant.Value)}, got len: {len(val)}')

            new_variant = ua.Variant(Value=val, VariantType=variant.VariantType, is_array=variant.is_array,
                                     Dimensions=variant.Dimensions)
            await nd.node.write_value(ua.DataValue(new_variant))
        except:
            # traceback.print_exc()
            raise


class OpcuaComms:

    # noinspection PyTypeChecker
    def __init__(self, opcua_endpoint: str, opcua_params: List[OpcuaDataParams]):
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
