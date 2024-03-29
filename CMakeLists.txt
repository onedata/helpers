cmake_minimum_required(VERSION 3.0.0)
project(helpers C CXX)

option(WITH_FUSE_VERSION "Fuse version to compile with (2 or 3)" 3)
option(WITH_CEPH "Include Ceph direct IO support" ON)
option(WITH_S3 "Include S3 direct IO support" ON)
option(WITH_SWIFT "Include Swift direct IO support" ON)
option(WITH_GLUSTERFS "Include GlusterFS direct IO support" ON)
option(WITH_WEBDAV "Include WebDAV and HTTP direct IO support" ON)
option(WITH_XROOTD "Include XRootD direct IO support" ON)
option(WITH_NFS "Include NFS direct IO support" ON)
option(WITH_TESTS "Build tests" ON)
option(BUILD_PROXY_IO "Build Proxy IO helper." ON)

# CMake config
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY True)
set(CMAKE_FIND_FRAMEWORK LAST)
set(CMAKE_POSITION_INDEPENDENT_CODE True)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FindLibraryUtils)

# Setup pthreads
message(STATUS "Checking for pthreads...")
find_package(Threads REQUIRED)

# Setup fuse, on Linux default required version is >= 3
# On other platforms it is not yet supported
if(WITH_FUSE_VERSION EQUAL 2)
    message(STATUS "Checking for FUSE 2...")
    find_package(FUSE 2 REQUIRED)
    set(FUSE_USE_VERSION 30)
else()
    message(STATUS "Checking for FUSE 3...")
    find_package(FUSE 3 REQUIRED)
    if(FUSE_VERSION_MINOR LESS 10)
        set(FUSE_USE_VERSION 31)
    else()
        set(FUSE_USE_VERSION 34)
    endif()

endif()
message(STATUS "Found Fuse version: ${FUSE_VERSION}")

# Setup Intel TBB
message(STATUS "Checking for Intel TBB...")
find_package(TBB REQUIRED)

# Setup glog
message(STATUS "Checking for glog...")
find_package(Glog REQUIRED)

# Setup CURL
message(STATUS "Checking for CURL...")
find_package(CURL REQUIRED)

# Setup NSS
message(STATUS "Checking for NSS...")
find_package(NSS REQUIRED)

# Setup Thread
message(STATUS "Checking for Threads...")
find_package(Threads REQUIRED)

# Setup OpenSSL
message(STATUS "Checking for OpenSSL...")
find_package(OpenSSL 1.1.0 REQUIRED)

# Setup Folly
message(STATUS "Checking for Folly...")
option(FOLLY_SHARED "Link folly as shared library" ON)
find_package(Folly REQUIRED)
find_package(Wangle REQUIRED)
find_package(Fizz REQUIRED)
find_library(LIBEVENT_LIBRARY event)
find_library(IBERTY_LIBRARY iberty)
find_library(DOUBLE_CONVERSION_LIBRARY double-conversion)
set(FOLLY_LIBRARIES
    ${FOLLY_LIBRARIES}
    ${LIBEVENT_LIBRARY}
    ${IBERTY_LIBRARY}
    ${WANGLE_LIBRARY}
    ${FIZZ_LIBRARY}
    ${DOUBLE_CONVERSION_LIBRARY})

# Setup Boost
message(STATUS "Checking for Boost components...")
set(Boost_USE_STATIC_LIBS       OFF)
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost COMPONENTS atomic chrono context date_time filesystem
                              iostreams log log_setup program_options regex
                              system thread REQUIRED)

# Setup Asio
set(ASIO_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/deps/asio/asio/include)
add_definitions(-DASIO_STANDALONE)

# Setup cppmetrics
set(CPPMETRICS_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/deps/cppmetrics)

# Setup spdlog and fmt
find_library(FMT_LIBRARY NAMES libfmt.a fmt REQUIRED)
find_library(SPDLOG_LIBRARY NAMES libspdlog.a spdlog REQUIRED)
list(APPEND HELPERS_LIBRARIES ${FMT_LIBRARY})
list(APPEND HELPERS_LIBRARIES ${SPDLOG_LIBRARY})
add_definitions(-DSPDLOG_COMPILED_LIB -DSPDLOG_FMT_EXTERNAL)

# Utility libs
find_library(LTDL_LIBRARY ltdl)
find_library(ZLIB_LIBRARY z)
find_library(RT_LIBRARY rt)
find_library(DL_LIBRARY dl)

# Rados library
if(WITH_CEPH)
    find_library(CEPHCOMMON_LIBRARY "ceph-common" NAMES "ceph-common" "libceph-common.so.2" "libceph-common.so.0" PATH_SUFFIXES "ceph")
    find_library(RADOS_LIBRARY rados)
    find_library(RADOSSTRIPER_LIBRARY radosstriper)
    add_definitions(-DWITH_CEPH=1)
else(WITH_CEPH)
    add_definitions(-DWITH_CEPH=0)
endif(WITH_CEPH)

# AWS SDK library
if(WITH_S3)
    find_library(AWS_SDK_C_COMMON_LIBRARY aws-c-common HINTS /usr/lib/x86_64-linux-gnu)
    find_library(AWS_SDK_CHECKSUMS_LIBRARY aws-checksums HINTS /usr/lib/x86_64-linux-gnu)
    find_library(AWS_SDK_C_EVENT_STREAM_LIBRARY aws-c-event-stream HINTS /usr/lib/x86_64-linux-gnu)
    find_library(AWS_SDK_CORE_LIBRARY aws-cpp-sdk-core HINTS /usr/lib/x86_64-linux-gnu)
    find_library(AWS_SDK_S3_LIBRARY aws-cpp-sdk-s3 HINTS /usr/lib/x86_64-linux-gnu)
    set(AWS_SDK_LIBRARIES
        ${AWS_SDK_C_COMMON_LIBRARY}
        ${AWS_SDK_CHECKSUMS_LIBRARY}
        ${AWS_SDK_C_EVENT_STREAM_LIBRARY}
        ${AWS_SDK_S3_LIBRARY}
        ${AWS_SDK_CORE_LIBRARY}
        ${CURL_LIBRARIES})
    add_definitions(-DWITH_S3=1)
    if(APPLE)
      add_definitions(-DS3_HAS_NO_V2_SUPPORT)
    endif(APPLE)
else(WITH_S3)
    add_definitions(-DWITH_S3=0)
endif(WITH_S3)

# Swift SDK library
if(WITH_SWIFT)
    find_library(SWIFT_SDK_LIBRARY Swift)
    find_library(POCO_FOUNDATION PocoFoundation)
    find_library(POCO_NET PocoNet)
    find_library(POCO_XML PocoXML)
    find_library(POCO_UTIL PocoUtil)
    set(SWIFT_SDK_LIBRARIES
        ${SWIFT_SDK_LIBRARY}
        ${POCO_FOUNDATION}
        ${POCO_NET}
        ${POCO_XML}
        ${POCO_UTIL})
    add_definitions(-DWITH_SWIFT=1)
else(WITH_SWIFT)
    add_definitions(-DWITH_SWIFT=0)
endif(WITH_SWIFT)

# GlusterFS SDK library
if(WITH_GLUSTERFS)
    include(FindPkgConfig)
    if (PKG_CONFIG_FOUND)
        set(ENV{PKG_CONFIG_PATH} "/opt/oneclient/lib/x86_64-linux-gnu/pkgconfig:$ENV{PKG_CONFIG_PATH}")
        pkg_check_modules(GLUSTERFSAPI glusterfs-api)
        if(NOT GLUSTERFSAPI_FOUND EQUAL 1)
            message(SEND_ERROR "pkg-config for glusterfs-api is missing.")
        else(NOT GLUSTERFSAPI_FOUND EQUAL 1)
            add_definitions(-DWITH_GLUSTERFS=1)
        endif(NOT GLUSTERFSAPI_FOUND EQUAL 1)
    endif (PKG_CONFIG_FOUND)
else(WITH_GLUSTERFS)
    add_definitions(-DWITH_GLUSTERFS=0)
endif(WITH_GLUSTERFS)

# WebDAV libraries
if(WITH_WEBDAV)
    find_library(PROXYGEN_LIBRARY proxygen REQUIRED)
    add_definitions(-DWITH_WEBDAV=1)
else(WITH_WEBDAV)
    add_definitions(-DWITH_WEBDAV=0)
endif(WITH_WEBDAV)

if(WITH_XROOTD)
    include(/usr/share/xrootd/cmake/XRootDConfig.cmake)
    find_package(XRootD REQUIRED)
    add_definitions(-DWITH_XROOTD=1)
else(WITH_XROOTD)
    add_definitions(-DWITH_XROOTD=0)
endif(WITH_XROOTD)

if(WITH_NFS)
    find_package(NFS REQUIRED)
    add_definitions(-DWITH_NFS=1)
else(WITH_NFS)
    add_definitions(-DWITH_NFS=0)
endif(WITH_NFS)


# Setup compile flags
set(PLATFORM_EXTRA_LIBS
    ${CMAKE_THREAD_LIBS_INIT}
    ${LTDL_LIBRARY}
    ${ZLIB_LIBRARY}
    ${DL_LIBRARY})

# Code coverage
option(CODE_COVERAGE "Enable code coverage (gcc only)." OFF)
if(CODE_COVERAGE)
    message("Code coverage enabled.")
    add_compile_options(--coverage)
    set(PLATFORM_EXTRA_LIBS ${PLATFORM_EXTRA_LIBS} gcov)
endif(CODE_COVERAGE)


if(APPLE)
    set(SECTION_FRAGMENTATION_FLAGS -Wno-deprecated-declarations)
    set(CUSTOM_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
else(NOT APPLE)
    set(PLATFORM_EXTRA_LIBS ${PLATFORM_EXTRA_LIBS} ${RT_LIBRARY})
    set(SECTION_FRAGMENTATION_FLAGS -fdata-sections -ffunction-sections)
    set(CUSTOM_RPATH "${CMAKE_INSTALL_PREFIX}/lib" "\$ORIGIN")
endif(APPLE)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -fdiagnostics-color")

add_compile_options(${SECTION_FRAGMENTATION_FLAGS} -Werror -Wshadow -Wall -Wpedantic)
add_definitions(
    ${FUSE_DEFINITIONS}
    -DFUSE_USE_VERSION=${FUSE_USE_VERSION}
    -DBOOST_FILESYSTEM_NO_DEPRECATED
    -DBOOST_ALL_DYN_LINK
    -DASIO_STANDALONE
    -DGLOG_STL_LOGGING_FOR_UNORDERED)

if(BUILD_PROXY_IO)
    add_definitions("-DBUILD_PROXY_IO")
endif(BUILD_PROXY_IO)

add_subdirectory(clproto)
add_subdirectory(src)

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/dummy.cc "")
set(HELPERS_SOURCES ${CMAKE_CURRENT_BINARY_DIR}/dummy.cc ${PROJECT_SOURCES})

# Define targets
add_library(helpersStatic STATIC ${HELPERS_SOURCES})
target_include_directories(helpersStatic SYSTEM PUBLIC ${HELPERS_SYSTEM_INCLUDE_DIRS})
target_include_directories(helpersStatic PUBLIC ${HELPERS_INCLUDE_DIRS})
set_target_properties(helpersStatic PROPERTIES OUTPUT_NAME helpers)

#
# Add helper libraries when building with each helper
#
if(WITH_CEPH)
    list(APPEND HELPERS_LIBRARIES ${RADOSSTRIPER_LIBRARY} ${RADOS_LIBRARY} ${CEPHCOMMON_LIBRARY})
endif(WITH_CEPH)

if(WITH_S3)
    list(APPEND HELPERS_LIBRARIES ${AWS_SDK_LIBRARIES})
endif(WITH_S3)

if(WITH_SWIFT)
    list(APPEND HELPERS_LIBRARIES ${SWIFT_SDK_LIBRARIES})
endif(WITH_SWIFT)

if(WITH_GLUSTERFS)
    list(APPEND HELPERS_LIBRARIES ${GLUSTERFSAPI_LIBRARIES})
endif(WITH_GLUSTERFS)

if(WITH_WEBDAV)
    list(APPEND HELPERS_LIBRARIES ${PROXYGEN_LIBRARY})
endif(WITH_WEBDAV)

if(WITH_XROOTD)
    list(APPEND HELPERS_LIBRARIES ${XROOTD_LIBRARIES})
endif(WITH_XROOTD)

if(WITH_NFS)
    list(APPEND HELPERS_LIBRARIES ${NFS_LIBRARIES})
endif(WITH_NFS)

add_library(helpersShared SHARED ${HELPERS_SOURCES})
target_link_libraries(helpersShared PUBLIC ${HELPERS_LIBRARIES})
target_include_directories(helpersShared SYSTEM PUBLIC ${HELPERS_SYSTEM_INCLUDE_DIRS})
target_include_directories(helpersShared PUBLIC ${HELPERS_INCLUDE_DIRS})
set_target_properties(helpersShared PROPERTIES OUTPUT_NAME helpers
    BUILD_WITH_INSTALL_RPATH true
    INSTALL_RPATH_USE_LINK_PATH true
    INSTALL_RPATH "${CUSTOM_RPATH}")

install(TARGETS helpersShared LIBRARY DESTINATION lib)
install(TARGETS helpersStatic ARCHIVE DESTINATION lib)
install(DIRECTORY include DESTINATION .)
install(DIRECTORY ${ASIO_INCLUDE_DIRS} DESTINATION .)
install(DIRECTORY ${CPPMETRICS_INCLUDE_DIRS}/cppmetrics DESTINATION include/)

find_program(
    CLANG_TIDY
    NAMES "run-clang-tidy"
    DOC "Path to run-clang-tidy script")

if(CLANG_TIDY)
    include(ProcessorCount)
    ProcessorCount(CLANG_TIDY_PARALLEL_JOBS)
    message(STATUS
        "run-clang-tidy script found: ${CLANG_TIDY} - adding target clang-tidy")
    set(CLANG_TIDY_SOURCE_FILTER "src/*.cc")
    file(GLOB_RECURSE CLANG_TIDY_SOURCES
         "${CMAKE_CURRENT_SOURCE_DIR}/${CLANG_TIDY_SOURCE_FILTER}")
    add_custom_target(clang-tidy COMMAND ${CLANG_TIDY}
        -export-fixes clang-tidy-suggested-fixes.yaml
        -j ${CLANG_TIDY_PARALLEL_JOBS}
        -extra-arg="-DTBB_USE_GLIBCXX_VERSION=50400"
        ${CLANG_TIDY_SOURCES})
else(CLANG_TIDY)
    message(STATUS "run-clang-tidy script not found - target clang-tidy not available")
endif(CLANG_TIDY)

if(WITH_TESTS)
    enable_testing()
    add_subdirectory(test)
endif(WITH_TESTS)
