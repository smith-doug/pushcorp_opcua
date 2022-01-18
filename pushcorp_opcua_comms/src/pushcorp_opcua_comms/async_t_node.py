#!/usr/bin/env python3

import asyncio
import random

import aiorospy
import rospy
import traceback

from asyncua import Client, ua, Node
from asyncua.common.subscription import Subscription
import asyncua.common.structures104

import asyncua.common.ua_utils as ua_utils
import asyncua.common.structures as acs
from codetiming import Timer


class TestStuff:

    # noinspection PyTypeChecker
    def __init__(self, opcua_endpoint: str):
        self.opcua_endpoint = opcua_endpoint
        self.client: Client = None
        self.event_loop = None

    async def keep_alive(self):
        while True:
            await asyncio.sleep(1.0)

    async def run(self):
        self.event_loop = asyncio.get_running_loop()
        self.client = Client(self.opcua_endpoint)

        await self.client.connect()
        try:

            rospy.loginfo(f'opcua client connected to {self.opcua_endpoint}')

            # Load types so they're available to the data objects
            gens = await self.client.load_data_type_definitions()

            node = self.client.get_node('ns=4;s=GVL_opcua.stTestBasic')

            kids = await ua_utils.get_node_children(node)
            print('')

            node_do = self.client.get_node('ns=4;s=GVL_opcua.stTestBasic.input.DO0')
            await node_do.read_value()

            node_di = self.client.get_node('ns=4;s=GVL_opcua.stTestBasic.output.DI0')
            di_val = await node_di.read_value()

            set_val = True
            rospy.loginfo(f'DO0 is wired to DI0.  Going to set DO0 = True and poll until DI0 is True.  DI0 has a 3ms '
                          f'debounce time.')

            with Timer():
                await node_do.write_value(ua.DataValue(ua.Variant(Value=set_val)))

                di_val = await node_di.read_value()
                read_cnt = 0
                while di_val != set_val:
                    read_cnt += 1
                    di_val = await node_di.read_value()

            rospy.loginfo(f'loop cnt: {read_cnt}')

            await node_do.write_value(ua.DataValue(ua.Variant(Value=False)))

            array_node = self.client.get_node('ns=4;s=GVL_opcua.stTestBasic.input.arrTest')
            array_node_data_val = await array_node.read_data_value()
            blag = array_node_data_val.Value

            variant: ua.Variant = None
            print('read array')
            with Timer():
                variant: ua.Variant = (await array_node.read_data_value()).Value

            print(variant)

            val = variant.Value
            val = [random.randint(0, 255) for b in val]

            new_variant = ua.Variant(Value=val, VariantType=variant.VariantType, is_array=variant.is_array,
                                     Dimensions=variant.Dimensions)
            for i in range(10):
                with Timer():
                    await array_node.write_value(ua.DataValue(new_variant))
            print('')

        except:
            traceback.print_exc()

    async def run_keba(self):
        self.event_loop = asyncio.get_running_loop()
        self.client = Client(self.opcua_endpoint)

        await self.client.connect()
        try:

            rospy.loginfo(f'opcua client connected to {self.opcua_endpoint}')

            # root_node = self.client.get_root_node()

            # Load types so they're available to the data objects
            gens = await self.client.load_data_type_definitions()

            node = self.client.get_node('ns=4;s=APPL.Application.GVL_opcua.stTestBasic')
            blah = await node.get_variables()
            kids = await ua_utils.get_node_children(node)
            print('')

        except:
            traceback.print_exc()

    async def testit(self):
        # self.client
        node = self.client.get_node('ns=4;s=GVL_opcua.stFcuControl')

        generators = await self.client.load_data_type_definitions(node, overwrite_existing=True)
        generators[0].save_to_file('/tmp/teststruct.py', True)

    async def recur(self, node: Node):
        pass


if __name__ == '__main__':
    rospy.init_node('async_test_node', anonymous=False)

    loop = asyncio.get_event_loop()
    loop.set_debug(False)

    ts = TestStuff("opc.tcp://192.168.125.39:4840")
    # ts = TestStuff("opc.tcp://192.168.72.3:4842")

    task = loop.create_task(ts.run())
    aiorospy.cancel_on_exception(task)
    aiorospy.cancel_on_shutdown(task)

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    except:
        traceback.print_exc()
