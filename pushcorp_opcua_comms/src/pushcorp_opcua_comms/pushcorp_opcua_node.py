#!/usr/bin/env python3
import asyncio

import rospy
import sys
import copy
import traceback
import pushcorp_msgs
import ros_opcua_srvs.srv
from rospy import ServiceProxy

from asyncua import Client, ua, Node
from asyncua.common.subscription import Subscription

OPCUA_ROOT = 'ns=4;s=GVL_opcua'
OPCUA_FCU = 'ns=4;s=GVL_opcua.stFcuControl'
OPCUA_SPINDLE = 'ns=4;s=GVL_opcua.stSpindleControl'


class FcuData:
    def __init__(self, client: Client):

        self.event_loop = asyncio.get_running_loop()
        self.client = client
        self.st_data = None
        self.hb_timer = None

        self.Heartbeat: Node = None
        self.st_data_node: Node = None
        self.sub_opcua: Subscription = None

        #self.map_data()

    async def map_data(self):
        print(OPCUA_FCU)

        self.st_data_node = self.client.get_node(OPCUA_FCU)
        self.st_data = await self.st_data_node.get_value()

        self.Heartbeat = self.client.get_node(OPCUA_FCU + '.Heartbeat')

        self.sub_opcua = await self.client.create_subscription(5, self)

        await self.sub_opcua.subscribe_data_change(self.st_data_node)

        self.hb_timer = rospy.Timer(rospy.Duration(1.0), self.heartbeat_cb)

    def datachange_notification(self, node, val, data):
        if node == self.st_data_node:
            self.st_data = val

        print(f'Datachange {self.st_data.Heartbeat}')

    async def heartbeat_co(self):
        val = self.st_data.Heartbeat
        print(val)

        await self.Heartbeat.set_value(ua.DataValue(not val))



    def heartbeat_cb(self, event):
        if self.Heartbeat is not None:
            #fut = self.event_loop.create_future()
            self.event_loop.create_task(self.heartbeat_co())

            #await fut



class PushcorpComms:
    opcua_srv_connect: ServiceProxy
    opcua_srv_disconnect: ServiceProxy

    def datachange_notification(self, node, val, data):
        print("Python: New data change event", node, val)

        if node == self.st_fcu_node:
            print('fcu node update')
            self.st_fcu = val
        elif node == self.st_spindle_node:
            print('spindle node update')
            self.st_spindle = val

    def event_notification(self, event):
        print("Python: New event", event)

    def __init__(self, opcua_endpoint: str):
        self.opcua_endpoint = opcua_endpoint

        self.event_loop = None
        self.client: Client = None
        self.objects: Node = None
        self.st_fcu_node: Node = None
        self.st_spindle_node: Node = None
        self.st_fcu = None
        self.st_spindle = None
        self.sub_opcua: Subscription = None
        self.sub_handle: Subscription = None

        self.st_fcu_input_node: Node = None

        self.fcu_data: FcuData = None

    #        rospy.Timer(rospy.Duration(1.0), self.heartbeat_cb)

    async def connect(self):
        self.event_loop = asyncio.get_running_loop()
        self.client = Client(self.opcua_endpoint)
        try:
            await self.client.connect()
            print('connected')
            await self.client.load_data_type_definitions()

            self.st_fcu_node = self.client.get_node(OPCUA_FCU)
            self.st_fcu = await self.st_fcu_node.get_value()

            self.st_fcu_input_node = self.client.get_node(OPCUA_FCU + '.input')

            self.st_spindle_node = self.client.get_node(OPCUA_SPINDLE)
            self.st_spindle = await self.st_spindle_node.get_value()

            # subscribing to a variable node

            # self.sub_opcua = self.client.create_subscription(500, self)

            # self.sub_handle = self.sub_opcua.subscribe_data_change([self.st_fcu_node, self.st_spindle_node])

            print(self.st_fcu.input.SetForce)

            self.fcu_data = FcuData(self.client)
            await self.fcu_data.map_data()


        except:
            traceback.print_exc()
        finally:
            pass

    async def run(self):
        await self.connect()

        while not rospy.is_shutdown():
            await asyncio.sleep(1.0)

    def disconnect(self):
        self.client.disconnect()

    def init_ros(self, opcua_endpoint: str):
        self.opcua_endpoint = opcua_endpoint

        self.opcua_srv_disconnect = rospy.ServiceProxy('/opcua/opcua_client/disconnect', ros_opcua_srvs.srv.Disconnect)
        self.opcua_srv_connect = rospy.ServiceProxy('/opcua/opcua_client/connect', ros_opcua_srvs.srv.Connect)

        rospy.loginfo('Calling disconnect on startup')
        dc_resp = self.opcua_srv_disconnect()

        # Connect to plc
        rospy.loginfo(f'Calling connect to {opcua_endpoint}')

        resp: ros_opcua_srvs.srv.ConnectResponse = self.opcua_srv_connect(self.opcua_endpoint)
        if not resp.success:
            rospy.logerr(f'Unable to connect to opcua endpoint: {resp.error_message}')
        else:
            rospy.loginfo('Connected to endpoint')

    def destroy(self):
        print("destroy")
        if self.opcua_srv_disconnect is not None:
            self.opcua_srv_disconnect()


if __name__ == '__main__':
    rospy.init_node('pushcorp_comms_node', anonymous=False)

    opcua_endpoint = ip = rospy.get_param('~opcua_endpoint', "opc.tcp://192.168.125.39:4840")

    pc = PushcorpComms(opcua_endpoint)

    asyncio.run(pc.run())
# pc.connect()
# rospy.spin()
# pc.disconnect()
