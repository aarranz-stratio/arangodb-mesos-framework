find_path(
    gflags_INCLUDE_DIR
      gflags.h
    PATH_SUFFIXES gflags
)

find_library(
    gflags_LIBRARY
      gflags
)

set(gflags_LIBRARIES ${gflags_LIBRARY})
set(gflags_INCLUDE_DIRS "${gflags_INCLUDE_DIR}")
set(gflags_DEFINITIONS "")


include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(gflags  DEFAULT_MSG
  gflags_LIBRARIES gflags_INCLUDE_DIR)
