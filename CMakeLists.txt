cmake_minimum_required(VERSION 3.20)
project(OrderBook)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Homebrew paths (Apple Silicon)
list(APPEND CMAKE_PREFIX_PATH
    /opt/homebrew/opt/mongo-cxx-driver
    /opt/homebrew/opt/mongo-c-driver
    /opt/homebrew/opt/librdkafka
    /opt/homebrew/opt/boost
    /opt/homebrew/opt/openssl
)

# Fetch cpp-httplib (HTTP framework)
include(FetchContent)
FetchContent_Declare(
    httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG        v0.18.5
)
FetchContent_MakeAvailable(httplib)

# Fetch nlohmann/json
FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        v3.11.3
)
FetchContent_MakeAvailable(nlohmann_json)

# MongoDB C++ driver
find_package(mongocxx REQUIRED)
find_package(bsoncxx REQUIRED)

# librdkafka
find_library(RDKAFKA_LIB rdkafka PATHS /opt/homebrew/opt/librdkafka/lib REQUIRED)
find_library(RDKAFKA_PP_LIB rdkafka++ PATHS /opt/homebrew/opt/librdkafka/lib REQUIRED)
find_path(RDKAFKA_INCLUDE_DIR librdkafka/rdkafkacpp.h PATHS /opt/homebrew/opt/librdkafka/include REQUIRED)

add_subdirectory(common)
add_subdirectory(order)
add_subdirectory(matching-engine)
