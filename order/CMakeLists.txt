add_executable(order-service
    src/main.cpp
    src/api/OrderController.cpp
    src/service/OrderService.cpp
    src/db/OrderRepository.cpp
    src/kafka/KafkaProducer.cpp
    src/outbox/OutboxRelay.cpp
)

target_include_directories(order-service PRIVATE
    ${RDKAFKA_INCLUDE_DIR}
    src
)

target_link_libraries(order-service PRIVATE
    common
    httplib::httplib
    mongo::mongocxx_shared
    mongo::bsoncxx_shared
    ${RDKAFKA_LIB}
    ${RDKAFKA_PP_LIB}
)
