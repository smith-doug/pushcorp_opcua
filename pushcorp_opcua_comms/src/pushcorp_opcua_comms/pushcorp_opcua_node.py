#!/usr/bin/env python3

import asyncio
import aiorospy
import rospy
import traceback
import pushcorp_msgs
import std_srvs.srv
from std_msgs.msg import String, Float32, Bool, Int32
from std_srvs.srv import SetBool, SetBoolRequest, SetBoolResponse
import pushcorp_msgs.srv
from enum import IntEnum

from asyncua import Client, ua, Node
from asyncua.common.subscription import Subscription

OPCUA_ROOT = 'ns=4;s=GVL_opcua'
OPCUA_FCU = 'ns=4;s=GVL_opcua.stFcuControl'
OPCUA_SPINDLE = 'ns=4;s=GVL_opcua.stSpindleControl'


class OpcuaDataParams:
    def __init__(self, nodeid='', ns='', topic_pub_pd=0.005, opcua_sub_pd=0.005):
        self.nodeid = nodeid
        self.ns = ns
        self.topic_pub_pd = topic_pub_pd
        self.opcua_sub_pd = opcua_sub_pd


class OpcuaData:

    # noinspection PyTypeChecker
    def __init__(self, client: Client, params: OpcuaDataParams):

        self.params = params
        self.ns = params.ns
        self.nodeid = params.nodeid
        self.event_loop = asyncio.get_running_loop()
        self.client: Client = client

        self.heartbeat: Node = None

        self.st_data = None  # Full data (hb, input, output), will be a struct created by magic
        self.st_data_node: Node = None  # Node the the full data
        self.st_data_input_node: Node = None  # Node to the input (writable) data
        self.sub_opcua: Subscription = None
        self.lock = asyncio.Lock()

        self.subs = {}  # Ros subscribers
        self.pubs = {}  # Ros publishers
        self.srvs = []  # Ros services

    async def map_data(self):
        rospy.loginfo(f'OpcuaData mapping with nodeid {self.nodeid}')

        self.st_data_node = self.client.get_node(self.nodeid)
        self.st_data = await self.st_data_node.get_value()
        # Input is a separate Node so that you don't overwrite outputs from the plc
        self.st_data_input_node = self.client.get_node(self.nodeid + '.input')

        # Heartbeat gets written separately but updating it will still cause the opcua subscriber to fire
        self.heartbeat = self.client.get_node(self.nodeid + '.Heartbeat')

        # Subscribe to the entire struct
        self.sub_opcua = await self.client.create_subscription(5, self)
        await self.sub_opcua.subscribe_data_change(self.st_data_node)
        self.event_loop.create_task(self.heartbeat_co())

        # Create topics and services for structure data
        in_vars = vars(self.st_data.input)
        out_vars = vars(self.st_data.output)

        # Create subscribers for input data
        for key, val in in_vars.items():
            sub = None
            topic_name = self.ns + '/' + str(key)
            if isinstance(val, float):
                sub = aiorospy.AsyncSubscriber(topic_name, Float32)
            elif isinstance(val, int) or isinstance(val, IntEnum):
                sub = aiorospy.AsyncSubscriber(topic_name, Int32)
            elif isinstance(val, bool):
                sub = aiorospy.AsyncSubscriber(topic_name, Bool)
            else:
                rospy.logerr(f'Unknown type {key}, {type(val)} in structure')

            if sub is not None:
                self.subs[str(key)] = sub

        # Create topic publishers for outputs
        for key, val in out_vars.items():
            pub = None
            topic_name = self.ns + '/' + str(key)
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

        # Create services
        for key, val in in_vars.items():
            srv = None
            topic_name = self.ns + '/' + str(key)
            name = str(key)

            if isinstance(val, float):
                srv = aiorospy.AsyncService(topic_name, pushcorp_msgs.srv.SetFloat32,
                                            (lambda req, name=name: self.svc_handler(req, str(name),
                                                                                     pushcorp_msgs.srv.SetFloat32Response)))
            elif isinstance(val, bool):
                srv = aiorospy.AsyncService(topic_name, std_srvs.srv.SetBool,
                                            lambda req, name=name: self.svc_handler(req, name,
                                                                                    std_srvs.srv.SetBoolResponse))
            elif isinstance(val, int) or isinstance(val, IntEnum):
                srv = aiorospy.AsyncService(topic_name, pushcorp_msgs.srv.SetInt32,
                                            lambda req, name=name: self.svc_handler(req, name,
                                                                                    pushcorp_msgs.srv.SetInt32Response))
            else:
                rospy.logerr(f'Unknown type {key}, {type(val)} in structure')

            if srv is not None:
                self.srvs.append(srv)

        self.event_loop.create_task(self.topic_listeners_init())
        self.event_loop.create_task(self.svc_handlers_init())
        self.event_loop.create_task(self.topic_publishers_init())

    async def svc_handler(self, req, name, ret_type):
        name = name
        rospy.loginfo(f'Updating {name} to {req.data}')
        await self.set_named_val(name, req.data)
        return ret_type(success=True)

    async def svc_handlers_init(self):
        await asyncio.gather(*[srv.start() for srv in self.srvs])

    async def topic_publisher(self, name, pub):
        try:
            while True:
                vals_current = self.st_data.output
                val = getattr(vals_current, name)
                pub.publish(val)
                await asyncio.sleep(0.005)
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
        if node == self.st_data_node:
            self.st_data = val

    async def heartbeat_co(self):
        while True:
            val = not self.st_data.Heartbeat
            try:
                await self.heartbeat.set_value(ua.DataValue(val))
            except asyncio.exceptions.TimeoutError:
                rospy.logerr(f"{self.ns} Heartbeat to opcua timed out")
                raise
            await asyncio.sleep(1)

    def get_input_vals(self):
        return self.st_data.input

    async def set_input_vals(self, data):
        async with self.lock:
            await self.st_data_input_node.write_value(ua.DataValue(data))

    def get_output_vals(self):
        return self.st_data.output

    async def set_named_val(self, name: str, value: any):

        vals_current = self.st_data.input
        setattr(vals_current, name, value)
        try:
            await self.set_input_vals(vals_current)
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

        self.fcu_data: OpcuaData = None
        self.spindle_data: OpcuaData = None

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
    loop.set_debug(True)

    opcua_ep = ip = rospy.get_param('~opcua_endpoint', "opc.tcp://192.168.125.39:4840")

    param_list = []
    fcu_params = OpcuaDataParams(nodeid='ns=4;s=GVL_opcua.stFcuControl', ns='/pushcorp/fcu')
    spindle_params = OpcuaDataParams(nodeid='ns=4;s=GVL_opcua.stSpindleControl', ns='/pushcorp/spindle')

    param_list.append(fcu_params)
    param_list.append(spindle_params)

    pc = OpcuaComms(opcua_ep, param_list)

    task = loop.create_task(pc.run())
    # aiorospy.cancel_on_exception(task)
    aiorospy.cancel_on_shutdown(task)

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    except:
        traceback.print_exc()
