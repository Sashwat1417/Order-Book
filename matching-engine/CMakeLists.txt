add_executable(matching-engine
    src/main/main.cpp
    src/main/listener/OrderListener.cpp
    src/main/producer/TradeProducer.cpp
    src/main/db/TradeRepository.cpp
    src/main/service/MatchingHelper.cpp
    src/main/service/OrderBook.cpp
    src/main/service/MatchingService.cpp
)

target_include_directories(matching-engine PRIVATE
    src/main
    src/main/listener
    src/main/producer
    src/main/db
    src/main/service
    ${RDKAFKA_INCLUDE_DIR}
)

target_link_libraries(matching-engine PRIVATE
    common
    mongo::mongocxx_shared
    mongo::bsoncxx_shared
    ${RDKAFKA_LIB}
    ${RDKAFKA_PP_LIB}
)
