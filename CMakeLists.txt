cmake_minimum_required(VERSION 3.10)

project(MP-Router)

# option(LINK_SO "Whether are linked dynamically" ON)
option(WITH_YASHANDB "Build YashanDB client support" ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result -fPIC")

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 20)

# 尝试部分静态链接，避免缺少 C++ 库，但不能全静态链接（因为部分库只有 .so）
# set(CMAKE_FIND_LIBRARY_SUFFIXES ".a") 
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++ -static-libgcc")

include(FindThreads)

include_directories($ENV{HOME}/local/include)
link_directories($ENV{HOME}/local/lib)
link_directories(/usr/lib/postgresql/14/lib)

if (WITH_YASHANDB)
    set(YASHANDB_ROOT "" CACHE PATH "YashanDB client installation root")
    set(YASHANDB_INCLUDE_DIR "" CACHE PATH "YashanDB client include directory")
    set(YASHANDB_LIBRARY "" CACHE FILEPATH "YashanDB yascli library")

    find_path(YASHANDB_INCLUDE_DIR
        NAMES yacli.h
        PATHS
            ${YASHANDB_INCLUDE_DIR}
            ${YASHANDB_ROOT}/include
            /root/yashandb-client/include
            /home/yashan/install/include
        NO_DEFAULT_PATH
    )
    find_library(YS_LIBRARY
        NAMES yascli libyascli
        PATHS
            ${YASHANDB_LIBRARY}
            ${YASHANDB_ROOT}/lib
            /root/yashandb-client/lib
            /home/yashan/install/lib
        PATH_SUFFIXES lib
        NO_DEFAULT_PATH
    )
    if (NOT YS_LIBRARY AND YASHANDB_LIBRARY)
        if (EXISTS ${YASHANDB_LIBRARY})
            set(YS_LIBRARY ${YASHANDB_LIBRARY})
        endif()
    endif()

    if (NOT YASHANDB_INCLUDE_DIR)
        message(FATAL_ERROR "YashanDB yacli.h not found! Set -DYASHANDB_INCLUDE_DIR=/path/to/include or -DWITH_YASHANDB=OFF")
    endif()
    if (NOT YS_LIBRARY)
        message(FATAL_ERROR "libyascli (YashanDB client) not found! Set -DYASHANDB_LIBRARY=/path/to/libyascli.so or -DWITH_YASHANDB=OFF")
    endif()
    message(STATUS "Found YashanDB includes: ${YASHANDB_INCLUDE_DIR}")
    message(STATUS "Found libyascli: ${YS_LIBRARY}")
    include_directories(${YASHANDB_INCLUDE_DIR})
else()
    message(STATUS "YashanDB client support disabled")
    set(YS_LIBRARY "")
endif()


# 在用户目录下寻找 metis.h
find_path(METIS_INCLUDE_DIR metis.h
    PATHS
    $ENV{HOME}/local/include/
    PATH_SUFFIXES include
)
if (METIS_INCLUDE_DIR)
    message(STATUS "Found METIS includes: ${METIS_INCLUDE_DIR}")
    include_directories(${METIS_INCLUDE_DIR})
else()
    message(FATAL_ERROR "METIS not found in user directory!")
endif()

# 查找 metis 库
find_library(METIS_LIBRARY
    NAMES metis libmetis
    PATHS
    $ENV{HOME}/local/lib
    PATH_SUFFIXES lib
)
if (METIS_LIBRARY) 
    message(STATUS "Found METIS library: ${METIS_LIBRARY}")
else()
    message(FATAL_ERROR "METIS library not found in user directory!")
endif()

# 查找 GKlib 库
find_library(GKLIB_LIBRARY
    NAMES GKlib libGKlib
    PATHS
    $ENV{HOME}/local/lib
    PATH_SUFFIXES lib
)
if (GKLIB_LIBRARY) 
    message(STATUS "Found GKlib library: ${GKLIB_LIBRARY}")
else()
    message(FATAL_ERROR "GKlib library not found in user directory!")
endif()

# 查找 PQXX 库
find_library(PQXX_LIBRARY
    NAMES pqxx libpqxx
    PATHS
    /usr/local/lib
    PATH_SUFFIXES lib
)
if (PQXX_LIBRARY) 
    message(STATUS "Found PQXX library: ${PQXX_LIBRARY}")
else()
    message(FATAL_ERROR "PQXX library not found in /usr/local/lib directory!")
endif()

# 查找 libpq
find_library(PQ_LIBRARY 
    pq PATHS 
    /usr/lib /usr/lib64 /usr/local/lib $ENV{HOME}/local/lib
    PATH_SUFFIXES lib
)
if (NOT PQ_LIBRARY)
    message(FATAL_ERROR "libpq (PostgreSQL client) not found!")
else()
    message(STATUS "Found libpq: ${PQ_LIBRARY}")
endif()

include_directories(./
        core
        thirdparty
        workload
        serve  # 添加serve目录以包含serve.cpp的头文件（如果有）
)

set(CONFIG_SRC config.cc)
add_library(global STATIC ${CONFIG_SRC})

# add_subdirectory(core)

add_subdirectory(serve)
