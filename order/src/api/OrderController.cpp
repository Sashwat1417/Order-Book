#include "OrderController.hpp"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// Helper: sets JSON body and status on a response
static void respond(httplib::Response& res, int status, const json& body) {
    res.status = status;
    res.set_content(body.dump(), "application/json");
}

OrderController::OrderController(OrderService& service)
    : service_(service)
{}

void OrderController::registerRoutes(httplib::Server& server) {

    // ── POST /order ──────────────────────────────────────────────────────────
    // Java analogy: @PostMapping("/order")
    server.Post("/order", [&](const httplib::Request& req, httplib::Response& res) {
        try {
            auto body  = json::parse(req.body);
            auto order = service_.placeOrder(body);
            respond(res, 201, order);
        } catch (const std::invalid_argument& e) {
            respond(res, 400, {{"error", e.what()}});
        } catch (const json::exception& e) {
            respond(res, 400, {{"error", std::string("Invalid JSON: ") + e.what()}});
        } catch (const std::exception& e) {
            respond(res, 500, {{"error", e.what()}});
        }
    });

    // ── GET /orderbook ───────────────────────────────────────────────────────
    // Java analogy: @GetMapping("/orderbook")
    server.Get("/orderbook", [&](const httplib::Request&, httplib::Response& res) {
        try {
            respond(res, 200, service_.getOrderBook());
        } catch (const std::exception& e) {
            respond(res, 500, {{"error", e.what()}});
        }
    });

    // ── DELETE /order/:id ────────────────────────────────────────────────────
    // Java analogy: @DeleteMapping("/order/{id}")
    server.Delete(R"(/order/([^/]+))", [&](const httplib::Request& req, httplib::Response& res) {
        try {
            std::string id = req.matches[1];
            if (service_.cancelOrder(id)) {
                respond(res, 200, {{"message", "Order cancelled"}});
            } else {
                respond(res, 404, {{"error", "Order not found"}});
            }
        } catch (const std::exception& e) {
            respond(res, 500, {{"error", e.what()}});
        }
    });
}
