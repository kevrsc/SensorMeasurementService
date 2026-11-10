package com.kevrsc.sensor;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.Map;
import java.util.TreeSet;

public final class MethodRouter implements HttpHandler {

    private final Map<String, HttpHandler> handlers;
    private final String allowed;

    public MethodRouter(Map<String, HttpHandler> handlers) {
        this.handlers = Map.copyOf(handlers);
        this.allowed = String.join(", ", new TreeSet<>(handlers.keySet()));
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        var handler = handlers.get(exchange.getRequestMethod());
        if (handler != null) {
            handler.handle(exchange);
            return;
        }
        try (exchange) {
            exchange.getResponseHeaders().set("Allow", allowed);
            exchange.sendResponseHeaders(405, -1);
        }
    }
}
