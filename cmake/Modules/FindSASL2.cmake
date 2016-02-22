find_path(
    SASL2_INCLUDE_DIR
      sasl.h
    PATH_SUFFIXES
      sasl
)

find_library(
    SASL2_LIBRARY
      sasl2
)

set(SASL2_LIBRARIES ${SASL2_LIBRARY})

set(SASL2_INCLUDE_DIRS "${SASL2_INCLUDE_DIR}")
set(SASL2_DEFINITIONS "")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SASL2  DEFAULT_MSG
  SASL2_LIBRARIES SASL2_INCLUDE_DIR)
