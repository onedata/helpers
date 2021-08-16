#
# - Try to find Facebook wangle library
# This will define
# FIZZ_FOUND
# FIZZ_INCLUDE_DIR
# FIZZ_LIBRARIES
#

find_package(Folly REQUIRED)

find_path(
    FIZZ_INCLUDE_DIR
    NAMES "fizz/client/AsyncFizzClient.h"
    HINTS
        "/usr/local/facebook/include"
)

find_library(
    FIZZ_LIBRARY
    NAMES fizz
    HINTS
        "/usr/local/facebook/lib"
)

set(FIZZ_LIBRARIES ${FIZZ_LIBRARY} ${FOLLY_LIBRARIES})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    FIZZ DEFAULT_MSG FIZZ_INCLUDE_DIR FIZZ_LIBRARIES)

mark_as_advanced(FIZZ_INCLUDE_DIR FIZZ_LIBRARIES FIZZ_FOUND)

if(FIZZ_FOUND AND NOT FIZZ_FIND_QUIETLY)
    message(STATUS "FIZZ: ${FIZZ_INCLUDE_DIR}")
endif()
