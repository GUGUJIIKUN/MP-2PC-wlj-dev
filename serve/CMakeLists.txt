include_directories(log)
include_directories(region)
# --- MODIFICATION START ---
# List all source files needed for the 'serve' executable
set(LOG_SRC
        log/Logger.cpp
)

set(METIS_SRC
        metis_partitioner.cc
)

set(SERVER_SRC
        serve.cpp
        region/region_generator.cpp
        # Add any other .cpp files needed directly by 'serve' here
)
set(MLP_SRC
        mlp/mlp.h
        mlp/mlp.cpp
        mlp/mlp_test.cpp
)
# --- MODIFICATION END ---

# add_executable(serve ${SERVER_SRC} ${LOG_SRC})
add_executable(mlp_test ${MLP_SRC})

add_library(metis STATIC ${METIS_SRC})
add_library(logger STATIC ${LOG_SRC})
add_library(mlp STATIC ${MLP_SRC})


add_subdirectory(test)

# Optional: If 'global' library already includes Logger and RegionProcessor objects,
# you might only need to link against global. But based on the errors,
# it seems 'serve' needs to compile these sources directly or link another library
# target that contains them. Adding them to SERVER_SRC is the direct approach.