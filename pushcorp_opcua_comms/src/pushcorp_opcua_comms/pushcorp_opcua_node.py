#!/usr/bin/env python3
import asyncio

from typing import Optional
import aiorospy
import rospy
import sys
import copy
import traceback
import pushcorp_msgs
import ros_opcua_srvs.srv
import std_srvs.srv
from rospy import ServiceProxy
from std_msgs.msg import String, Float32, Bool, Int32
from std_srvs.srv import SetBool, SetBoolRequest, SetBoolResponse
import pushcorp_msgs.srv
from enum import IntEnum

from asyncua import Client, ua, Node
from asyncua.common.subscription import Subscription

OPCUA_ROOT = 'ns=4;s=GVL_opcua'
OPCUA_FCU = 'ns=4;s=GVL_opcua.stFcuControl'
OPCUA_SPINDLE = 'ns=4;s=GVL_opcua.stSpindleControl'


class OpcuaData:

    # noinspection PyTypeChecker
    def __init__(self, client: Client, node_uri: str, ns: str):

        self.ns = ns
        self.node_uri = node_uri
        self.event_loop = asyncio.get_running_loop()
        self.client: Client = client

        self.Heartbeat: Node = None

        self.st_data = None  # Full data (hb, input, output)
        self.st_data_node: Node = None
        self.st_data_input_node: Node = None
        self.sub_opcua: Subscription = None
        self.lock = asyncio.Lock()
        self.subs = {}
        self.pubs = {}
        self.srvs = []





    async def map_data(self):
        print(self.node_uri)

        self.st_data_node = self.client.get_node(self.node_uri)
        self.st_data = await self.st_data_node.get_value()
        self.st_data_input_node = self.client.get_node(self.node_uri + '.input')

        self.Heartbeat = self.client.get_node(self.node_uri + '.Heartbeat')

        self.sub_opcua = await self.client.create_subscription(5, self)

        await self.sub_opcua.subscribe_data_change(self.st_data_node)
        self.event_loop.create_task(self.heartbeat_co())

        self.st_data = await self.st_data_node.get_value()

        in_vars = vars(self.st_data.input)
        out_vars = vars(self.st_data.output)
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
                print(f'Unknown type {key}, {type(val)} in structure')

            if sub is not None:
                self.subs[str(key)] = sub
                #self.subs.append(sub)

        for key, val in out_vars.items():
            pub = None
            topic_name = self.ns + '/' + str(key)
            if isinstance(val, float):
                pub = rospy.Publisher(topic_name, Float32, queue_size=1)
            elif isinstance(val, int) or isinstance(val, IntEnum):
                pub = rospy.Publisher(topic_name, Int32, queue_size=1)
            elif isinstance(val, bool):
                pub = rospy.Publisher(topic_name, Bool, queue_size=1)
            else:
                print(f'Unknown type {key}, {type(val)} in structure')

            if pub is not None:
                self.pubs[str(key)] = pub

        for key, val in in_vars.items():
            srv = None
            topic_name = self.ns + '/' + str(key)

            if type(val) is float:
                srv = aiorospy.AsyncService(topic_name, pushcorp_msgs.srv.SetFloat32,
                                            lambda req: self.svc_handler(req=req, name=key,
                                                                         ret_type=pushcorp_msgs.srv.SetFloat32Response))
            elif type(val) is int or isinstance(val, IntEnum):
                srv = aiorospy.AsyncService(topic_name, pushcorp_msgs.srv.SetInt32,
                                            lambda req: self.svc_handler(req=req, name=key,
                                                                         ret_type=pushcorp_msgs.srv.SetInt32Response))
            elif type(val) is bool:
                srv = aiorospy.AsyncService(topic_name, std_srvs.srv.SetBool,
                                            lambda req: self.svc_handler(req=req, name=key,
                                                                         ret_type=std_srvs.srv.SetBoolResponse))
            else:
                print(f'Unknown type {key}, {type(val)} in structure')

            if srv is not None:
                self.srvs.append(srv)

        asyncio.create_task(self.topic_listeners_init())
        asyncio.create_task(self.svc_handlers_init())
        asyncio.create_task(self.topic_publishers_init())


    async def svc_handler(self, req, name, ret_type):
        rospy.loginfo(f'Updating {name} to {req.data}')
        await self.set_named_val(name, req.data)
        return ret_type(success=True)

    async def svc_handlers_init(self):
        await asyncio.gather(*[srv.start() for srv in self.srvs])

    async def topic_publisher(self, name, pub):
        while True:
            vals_current = self.st_data.output
            val = getattr(vals_current, name)
            pub.publish(val)
            await asyncio.sleep(0.005)


    async def topic_publishers_init(self):
        await asyncio.gather(*[self.topic_publisher(pub[0], pub[1]) for pub in self.pubs.items()])

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
                await self.Heartbeat.set_value(ua.DataValue(val))
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

class PushcorpComms:

    # def event_notification(self, event):
    #     print("Python: New event", event)

    # noinspection PyTypeChecker
    def __init__(self, opcua_endpoint: str):
        self.opcua_endpoint = opcua_endpoint

        self.event_loop = None
        self.client: Client = None
        self.st_fcu_node: Node = None
        self.st_spindle_node: Node = None
        self.st_fcu = None
        self.st_spindle = None
        self.sub_opcua: Subscription = None
        self.sub_handle: Subscription = None

        self.st_fcu_input_node: Node = None

        self.fcu_data: OpcuaData = None
        self.spindle_data: OpcuaData = None

    async def setbool_svc(self, req: SetBoolRequest) -> SetBoolResponse:
        resp = SetBoolResponse()
        resp.message = 'Neat'
        resp.success = not req.data
        return resp

    async def listener(self):

        sub = aiorospy.AsyncSubscriber('~/blah', String)

        async for message in sub.subscribe():
            await self.fcu_data.set_named_val('SetForce', 15)

    async def connect(self):
        self.event_loop = asyncio.get_running_loop()
        self.client = Client(self.opcua_endpoint)
        try:
            await self.client.connect()
            print('connected')
            await self.client.load_data_type_definitions()

            self.st_spindle_node = self.client.get_node(OPCUA_SPINDLE)
            self.st_spindle = await self.st_spindle_node.get_value()

            self.server = aiorospy.AsyncService('service', SetBool, self.setbool_svc)

            self.fcu_data = OpcuaData(self.client, OPCUA_FCU, '/pushcorp/fcu')

            self.spindle_data = OpcuaData(self.client, OPCUA_SPINDLE, '/pushcorp/spindle')


            await asyncio.gather(self.listener(),
                                 self.fcu_data.map_data(),
                                 self.spindle_data.map_data(),
                                 self.server.start())

        except asyncio.CancelledError:
            raise
        except:
            traceback.print_exc()
        finally:
            pass

    async def run(self):
        await self.connect()

    def disconnect(self):
        self.client.disconnect()


if __name__ == '__main__':
    rospy.init_node('pushcorp_comms_node', anonymous=False)

    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    opcua_ep = ip = rospy.get_param('~opcua_endpoint', "opc.tcp://192.168.125.39:4840")

    pc = PushcorpComms(opcua_ep)

    task = loop.create_task(pc.run())
    #aiorospy.cancel_on_exception(task)
    aiorospy.cancel_on_shutdown(task)

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    except:
        traceback.print_exc()
