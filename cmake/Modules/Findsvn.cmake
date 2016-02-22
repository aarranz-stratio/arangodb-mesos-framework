
find_path(
    svn_INCLUDE_DIR
      svn_delta.h
    PATH_SUFFIXES
      subversion-1
)

find_library(
    svn_delta_LIBRARY
      svn_delta-1
)

find_library(
    svn_subr_LIBRARY
      svn_subr-1
)

set(svn_LIBRARIES ${svn_delta_LIBRARY} ${svn_subr_LIBRARY})
set(svn_INCLUDE_DIRS "${svn_INCLUDE_DIR}")
set(svn_DEFINITIONS "")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(svn  DEFAULT_MSG
    svn_LIBRARIES apr_INCLUDE_DIR)
