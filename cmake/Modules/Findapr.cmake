find_path(
    apr_INCLUDE_DIR
      apr.h
    PATH_SUFFIXES apr-1
)

find_library(
    apr_LIBRARY
      apr-1
)

set(apr_LIBRARIES ${apr_LIBRARY})
set(apr_INCLUDE_DIRS "${apr_INCLUDE_DIR}")
set(apr_DEFINITIONS "")


include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(apr  DEFAULT_MSG
  apr_LIBRARIES apr_INCLUDE_DIR)
