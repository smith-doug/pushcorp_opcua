cmake_minimum_required(VERSION 2.8.3)
project(pushcorp_msgs)

find_package(catkin REQUIRED COMPONENTS
  message_generation
  std_msgs
  std_srvs  
)

add_message_files(
  FILES
  Int32Array.msg
)

add_service_files(
  FILES
  SetFloat32.srv
  SetInt32.srv
  SetString.srv
  SetInt32Array.srv
)

generate_messages(
  DEPENDENCIES
  std_msgs
  std_srvs    
)

catkin_package(
 CATKIN_DEPENDS message_runtime std_msgs std_srvs   
)