cmake_minimum_required (VERSION 3.0)
project (arangodb-mesos-framework CXX)
include(FindPackageHandleStandardArgs)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/bin)

# manually set c++11 because by default cmake would use gnu++11 and that has a global
# #define unix 1 which is breaking libprocess... 
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
if(COMPILER_SUPPORTS_CXX11)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
endif()

add_executable(
  arangodb-mesos-framework
  src/framework.cpp
)

add_library(
  arangodb-mesos STATIC
  src/zookeeper.cpp
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

find_package(apr REQUIRED)
find_package(Threads REQUIRED)
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(apr REQUIRED)
find_package(svn REQUIRED)
find_package(SASL2 REQUIRED)
find_package(Libunwind REQUIRED)
find_package(Libmicrohttpd REQUIRED)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/3rdParty/pbjson/src
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/protobuf-3.3.0/src/protobuf-3.3.0-lib/lib/include
  ${MESOS_FOUND_SOURCE_PATH}/build/include/
  ${MESOS_FOUND_SOURCE_PATH}/include
  ${MESOS_FOUND_SOURCE_PATH}/include/mesos
  ${MESOS_FOUND_SOURCE_PATH}/3rdparty/libprocess/include/
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/boost-1.53.0/src/boost-1.53.0/
  ${MESOS_FOUND_SOURCE_PATH}/3rdparty/stout/include
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/glog-0.3.3/src/glog-0.3.3-lib/lib/include/
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/picojson-1.3.0/src/picojson-1.3.0/
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/zookeeper-3.4.8/src/zookeeper-3.4.8/src/c/include/zookeeper
  ${MESOS_FOUND_SOURCE_PATH}/src
  ${Libmicrohttpd_INCLUDE_DIR}
  ${CURL_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR}
)

target_link_libraries(
  arangodb-mesos-framework
  arangodb-mesos
)

#set(LIBS
#  ${MESOS_FOUND_SOURCE_PATH}/build/src/libmesos-1.4.0.a
#  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/libprocess/.libs/libprocess.a
#  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/libprocess/3rdparty/glog-0.3.3/.libs/libglog.a
#  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/libprocess/3rdparty/protobuf-2.5.0/src/.libs/libprotobuf.a
##  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/zookeeper-3.4.5/src/c/.libs/libzookeeper_mt.a
 # ${MESOS_LIB_LEVELDB}
#  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/libprocess/3rdparty/libev-4.15/.libs/libev.a
#  ${CURL_LIBRARIES}
#  ${OPENSSL_LIBRARIES}
#  ${apr_LIBRARIES}
#  ${svn_LIBRARIES}
#  ${ZLIB_LIBRARIES}
#  ${SASL2_LIBRARIES}
#  ${Libunwind_LIBRARIES}
#  ${Libmicrohttpd_LIBRARIES}
#  ${CMAKE_DL_LIBS}
#  ${CMAKE_THREAD_LIBS_INIT}
#)

set(LIBS
  ${MESOS_FOUND_SOURCE_PATH}/build/src/libmesos-1.4.0.a
  ${MESOS_FOUND_SOURCE_PATH}/build/src/libmesos-protobufs.a
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/libprocess/src/libprocess-0.0.1.a
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/protobuf-3.3.0/src/protobuf-3.3.0-lib/lib/lib/libprotobuf.a
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/glog-0.3.3/src/glog-0.3.3-lib/lib/lib/libglog.a
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/libev-4.22/src/libev-4.22-lib/lib/libev.a
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/http_parser-2.6.2/src/http_parser-2.6.2-build/libhttp_parser.a
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/leveldb-1.19/src/leveldb-1.19/out-static/libleveldb.a
  ${MESOS_FOUND_SOURCE_PATH}/build/3rdparty/zookeeper-3.4.8/src/zookeeper-3.4.8/src/c/lib/libzookeeper_mt.a
  ${apr_LIBRARIES}
  ${svn_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  ${CURL_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${SASL2_LIBRARIES}
  ${Libmicrohttpd_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CMAKE_THREAD_LIBS_INIT}
)

target_link_libraries(
  arangodb-mesos
  ${LIBS}
)

add_custom_command(
  OUTPUT ${CMAKE_SOURCE_DIR}/src/arangodb.pb.cc ${CMAKE_SOURCE_DIR}/src/arangodb.pb.h
  COMMAND ${CMAKE_BINARY_DIR}/mesos/build/3rdparty/protobuf-3.3.0/src/protobuf-3.3.0-lib/lib/bin/protoc -I${CMAKE_SOURCE_DIR}/src -I${CMAKE_BINARY_DIR}/mesos/include --cpp_out=${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/src/arangodb.proto
  DEPENDS ${CMAKE_SOURCE_DIR}/src/arangodb.proto
)
