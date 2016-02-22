find_path(
  Libmicrohttpd_INCLUDE_DIR
      microhttpd.h
)

find_library(
  Libmicrohttpd_LIBRARY
      microhttpd
)

set(Libmicrohttpd_LIBRARIES ${Libmicrohttpd_LIBRARY})
set(Libmicrohttpd_INCLUDE_DIRS "${Libmicrohttpd_INCLUDE_DIR}")
set(Libmicrohttpd_DEFINITIONS "")


include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Libmicrohttpd  DEFAULT_MSG
  Libmicrohttpd_LIBRARIES Libmicrohttpd_INCLUDE_DIR)
