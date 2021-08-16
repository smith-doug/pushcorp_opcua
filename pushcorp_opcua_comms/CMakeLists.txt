cmake_minimum_required(VERSION 2.8.3)
project(pushcorp_opcua_comms)

find_package(catkin REQUIRED COMPONENTS
  rospy
  pushcorp_msgs
)

catkin_python_setup()

catkin_package(
  CATKIN_DEPENDS
    rospy
    pushcorp_msgs
)

#############
## Install ##
#############

catkin_install_python(
  PROGRAMS
    src/pushcorp_opcua_comms/pushcorp_opcua_node.py
    #src/pushcorp_opcua_comms/async_test.py
  DESTINATION
    ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY launch/
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/launch)
