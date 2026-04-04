add_executable(matching-engine src/main.cpp)

target_include_directories(matching-engine PRIVATE
    ${RDKAFKA_INCLUDE_DIR}
)

target_link_libraries(matching-engine PRIVATE
    common
    mongo::mongocxx_shared
    mongo::bsoncxx_shared
    ${RDKAFKA_LIB}
    ${RDKAFKA_PP_LIB}
)
