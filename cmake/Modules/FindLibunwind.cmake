find_path(
  Libunwind_INCLUDE_DIR
      unwind.h
)

find_library(
  Libunwind_LIBRARY
      unwind
)

set(Libunwind_LIBRARIES ${Libunwind_LIBRARY})

set(Libunwind_INCLUDE_DIRS "${Libunwind_INCLUDE_DIR}")
set(Libunwind_DEFINITIONS "")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Libunwind  DEFAULT_MSG
  Libunwind_LIBRARIES Libunwind_INCLUDE_DIR)
