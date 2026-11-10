package com.kevrsc.sensor.http;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(10)
class MethodRouterTest {

    private final HttpClient client = HttpClient.newHttpClient();
    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void sendsEachMethodToItsHandlerAndRejectsTheRest() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", new MethodRouter(Map.of("GET", replyWith(200), "POST", replyWith(204))));
        server.start();

        assertEquals(200, send("GET").statusCode());
        assertEquals(204, send("POST").statusCode());
        var put = send("PUT");
        assertEquals(405, put.statusCode());
        assertEquals("GET, POST", put.headers().firstValue("Allow").orElseThrow());
    }

    private static HttpHandler replyWith(int status) {
        return exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(status, -1);
            }
        };
    }

    private HttpResponse<String> send(String method) throws Exception {
        var uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/readings");
        var request = HttpRequest.newBuilder(uri).method(method, HttpRequest.BodyPublishers.ofString("{}")).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
