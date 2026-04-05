find_package(CURL REQUIRED)

add_executable(notification-service
    src/main/main.cpp
    src/main/listener/TradeListener.cpp
    src/main/service/NotificationService.cpp
    src/main/service/EmailHelper.cpp
)

target_include_directories(notification-service PRIVATE
    src/main
    src/main/listener
    src/main/service
    ${RDKAFKA_INCLUDE_DIR}
)

target_link_libraries(notification-service PRIVATE
    common
    ${RDKAFKA_LIB}
    ${RDKAFKA_PP_LIB}
    CURL::libcurl
)
