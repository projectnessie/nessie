/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.nessie.client.http;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class TestHttpClient {

  private static final Executor EXEC = Executors.newFixedThreadPool(20);

  private static HttpRequest get(InetSocketAddress address) {
    return new HttpClient("http://localhost:" + address.getPort()).create();
  }

  @Test
  void testGet() {
    HttpHandler handler = h -> {
      Assertions.assertEquals("GET", h.getRequestMethod());
      String response = "Hi there!";
      h.sendResponseHeaders(200, response.getBytes().length);//response code and length
      OutputStream os = h.getResponseBody();
      os.write(response.getBytes());
      os.close();
    };
    try (TestServer server = new TestServer(handler)) {
      get(server.server.getAddress()).get();
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  private static class TestServer implements AutoCloseable {
    private final HttpServer server;

    TestServer(HttpHandler handler) throws IOException {
      server = HttpServer.create(new InetSocketAddress("localhost",0), 0);
      server.createContext("/", handler);
      server.setExecutor(EXEC);
      server.start();
    }

    @Override
    public void close() throws Exception {
      server.stop(0);
    }
  }
}
