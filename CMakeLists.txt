cmake_minimum_required (VERSION 2.6)
project (arangodb-mesos-framework)
include(FindPackageHandleStandardArgs)

add_executable(
  arangodb-mesos-framework
  src/framework.cpp
)

add_library(
  libarangodb-mesos STATIC
	src/ArangoManager.cpp 
	src/ArangoScheduler.cpp 
	src/ArangoState.cpp 
	src/Caretaker.cpp 
	src/CaretakerStandalone.cpp 
	src/CaretakerCluster.cpp 
	src/Global.cpp 
	src/HttpServer.cpp 
	src/arangodb.pb.cc 
	src/utils.cpp 
	3rdParty/pbjson/src/pbjson.cpp
)

target_compile_features(arangodb-mesos-framework PRIVATE cxx_constexpr)
target_compile_features(libarangodb-mesos PRIVATE cxx_constexpr)

if(NOT MESOS_SOURCE_PATH)
  set(MESOS_SOURCE_PATH ${CMAKE_CURRENT_BINARY_DIR}/mesos)
endif()

find_path(MESOS_FOUND_SOURCE_PATH
  NAMES build/3rdparty/libprocess/3rdparty/stout/include/Makefile
  PATHS ${MESOS_SOURCE_PATH}
  NO_CMAKE_SYSTEM_PATH
)

if(NOT MESOS_FOUND_SOURCE_PATH)
  message(FATAL_ERROR "Please provide -DMESOS_SOURCE_PATH or link the mesos source directory to ${CMAKE_BINARY_DIR}/mesos")
endif ()

find_path(MESOS_INCLUDE_DIR
  mesos/mesos.hpp
)

find_library(MESOS_LIBRARY
  NAMES mesos
)

find_package_handle_standard_args(Mesos  DEFAULT_MSG
  MESOS_LIBRARY MESOS_INCLUDE_DIR)

if ( NOT Mesos_FOUND)
  message(FATAL_ERROR "Mesos not found!")
endif()

find_path(MHD_INCLUDE_DIR
  libmicrohttpd/platform.h
)
find_library(MHD_LIBRARY
  microhttpd
)
find_package_handle_standard_args(Microhttpd  DEFAULT_MSG
  MHD_LIBRARY MHD_INCLUDE_DIR)

if ( NOT Microhttpd_FOUND)
  message(FATAL_ERROR "libmicrohttpd not found!")
endif()


find_package (Threads REQUIRED)
find_package (CURL REQUIRED)
find_package(Boost COMPONENTS regex REQUIRED)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/3rdParty/pbjson/src
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/libprocess/3rdparty/stout/include
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/libprocess/include
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/zookeeper-3.4.5/src/c/include
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/zookeeper-3.4.5/src/c/generated
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/libprocess/3rdparty/protobuf-2.5.0/src
  ${MESOS_FOUND_SOURCE_PATH}/build/src
  ${MESOS_FOUND_SOURCE_PATH}/src
  ${MESOS_INCLUDE_DIR}
  ${MHD_INCLUDE_DIR}
  ${CURL_INCLUDE_DIR}
)

target_link_libraries(
  arangodb-mesos-framework
  libarangodb-mesos
  ${MESOS_LIBRARY}
  ${MHD_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT}
  ${CURL_LIBRARIES}
  ${Boost_REGEX_LIBRARY}
)
