#pragma once

#include "../service/OrderService.hpp"
#include <httplib.h>

// Java analogy: equivalent to a Spring @RestController
class OrderController {
public:
    explicit OrderController(OrderService& service);

    // Registers all routes on the HTTP server
    // Java analogy: like having @RequestMapping methods auto-registered
    void registerRoutes(httplib::Server& server);

private:
    OrderService& service_;
};
