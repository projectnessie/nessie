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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class TestHttpClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static HttpRequest get(InetSocketAddress address) {
    return new HttpClient("http://localhost:" + address.getPort()).create();
  }

  @Test
  void testGet() {
    ExampleBean inputBean = new ExampleBean("x", 1);
    HttpHandler handler = h -> {
      Assertions.assertEquals("GET", h.getRequestMethod());
      String response = MAPPER.writeValueAsString(inputBean);
      h.sendResponseHeaders(200, response.getBytes().length);
      OutputStream os = h.getResponseBody();
      os.write(response.getBytes());
      os.close();
    };
    try (TestServer server = new TestServer(handler)) {
      ExampleBean bean = get(server.server.getAddress()).get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  @Test
  void testPut() {
    ExampleBean inputBean = new ExampleBean("x", 1);
    HttpHandler handler = h -> {
      Assertions.assertEquals("PUT", h.getRequestMethod());
      Object bean = MAPPER.readerFor(ExampleBean.class).readValue(h.getRequestBody());
      Assertions.assertEquals(inputBean, bean);
      h.sendResponseHeaders(200, 0);
    };
    try (TestServer server = new TestServer(handler)) {
      get(server.server.getAddress()).put(inputBean);
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  @Test
  void testPost() {
    ExampleBean inputBean = new ExampleBean("x", 1);
    HttpHandler handler = h -> {
      Assertions.assertEquals("POST", h.getRequestMethod());
      Object bean = MAPPER.readerFor(ExampleBean.class).readValue(h.getRequestBody());
      Assertions.assertEquals(inputBean, bean);
      h.sendResponseHeaders(200, 0);
    };
    try (TestServer server = new TestServer(handler)) {
      get(server.server.getAddress()).post(inputBean);
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  @Test
  void testDelete() {
    HttpHandler handler = h -> {
      Assertions.assertEquals("DELETE", h.getRequestMethod());
      h.sendResponseHeaders(200, 0);
    };
    try (TestServer server = new TestServer(handler)) {
      get(server.server.getAddress()).delete();
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  @Test
  void testGetQueryParam() {
    ExampleBean inputBean = new ExampleBean("x", 1);
    HttpHandler handler = h -> {
      Assertions.assertEquals("x=y", h.getRequestURI().getQuery());
      Assertions.assertEquals("GET", h.getRequestMethod());
      String response = MAPPER.writeValueAsString(inputBean);
      h.sendResponseHeaders(200, response.getBytes().length);
      OutputStream os = h.getResponseBody();
      os.write(response.getBytes());
      os.close();
    };
    try (TestServer server = new TestServer(handler)) {
      ExampleBean bean = get(server.server.getAddress()).queryParam("x", "y").get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  @Test
  void testGetMultipleQueryParam() {
    ExampleBean inputBean = new ExampleBean("x", 1);
    HttpHandler handler = h -> {
      String[] queryParams = h.getRequestURI().getQuery().split("&");
      Assertions.assertEquals(2, queryParams.length);
      Set<String> queryParamSet = new HashSet<>(Arrays.asList(queryParams));
      Assertions.assertTrue(queryParamSet.contains("x=y"));
      Assertions.assertTrue(queryParamSet.contains("a=b"));
      Assertions.assertEquals("GET", h.getRequestMethod());
      String response = MAPPER.writeValueAsString(inputBean);
      h.sendResponseHeaders(200, response.getBytes().length);
      OutputStream os = h.getResponseBody();
      os.write(response.getBytes());
      os.close();
    };
    try (TestServer server = new TestServer(handler)) {
      ExampleBean bean = get(server.server.getAddress())
          .queryParam("x", "y").queryParam("a", "b").get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  @Test
  void testGetNullQueryParam() {
    ExampleBean inputBean = new ExampleBean("x", 1);
    HttpHandler handler = h -> {
      String queryParams = h.getRequestURI().getQuery();
      Assertions.assertEquals(1, queryParams.length());
      Assertions.assertEquals("x", queryParams);
      Assertions.assertEquals("GET", h.getRequestMethod());
      String response = MAPPER.writeValueAsString(inputBean);
      h.sendResponseHeaders(200, response.getBytes().length);
      OutputStream os = h.getResponseBody();
      os.write(response.getBytes());
      os.close();
    };
    try (TestServer server = new TestServer(handler)) {
      ExampleBean bean = get(server.server.getAddress())
          .queryParam("x", null).get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  @Test
  void testGetTemplate() {
    ExampleBean inputBean = new ExampleBean("x", 1);
    HttpHandler handler = h -> {
      Assertions.assertEquals("GET", h.getRequestMethod());
      String response = MAPPER.writeValueAsString(inputBean);
      h.sendResponseHeaders(200, response.getBytes().length);
      OutputStream os = h.getResponseBody();
      os.write(response.getBytes());
      os.close();
    };
    try (TestServer server = new TestServer("/a/b", handler)) {
      ExampleBean bean = get(server.server.getAddress()).path("a/b").get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
      bean = get(server.server.getAddress()).path("a/{b}").resolveTemplate("b", "b").get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  @Test
  void testGetTemplateThrows() {
    HttpHandler handler = h -> Assertions.fail();
    try (TestServer server = new TestServer("/a/b", handler)) {
      Assertions.assertThrows(HttpClientException.class,
          () -> get(server.server.getAddress()).path("a/{b}").get().readEntity(ExampleBean.class));
      Assertions.assertThrows(IllegalStateException.class,
          () -> get(server.server.getAddress()).path("a/b").resolveTemplate("b", "b")
                                                                   .get().readEntity(ExampleBean.class));
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  @Test
  void testFilters() {
    AtomicBoolean requestFilterCalled = new AtomicBoolean(false);
    AtomicBoolean responseFilterCalled = new AtomicBoolean(false);
    HttpHandler handler = h -> {
      Assertions.assertTrue(h.getRequestHeaders().containsKey("x"));
      h.sendResponseHeaders(200, 0);
    };
    try (TestServer server = new TestServer(handler)) {
      HttpClient client = new HttpClient("http://localhost:" + server.server.getAddress().getPort());
      client.register((RequestFilter) context -> {
        requestFilterCalled.set(true);
        context.getHeaders().put("x", "y");
      });
      client.register((ResponseFilter) con -> {
        try {
          Assertions.assertEquals(Status.OK, con.getResponseCode());
          responseFilterCalled.set(true);
        } catch (IOException e) {
          Assertions.fail();
        }
      });
      client.create().get();
      Assertions.assertTrue(responseFilterCalled.get());
      Assertions.assertTrue(requestFilterCalled.get());
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  @Test
  void testHeaders() {
    HttpHandler handler = h -> {
      Assertions.assertTrue(h.getRequestHeaders().containsKey("x"));
      h.sendResponseHeaders(200, 0);
    };
    try (TestServer server = new TestServer(handler)) {
      get(server.server.getAddress()).header("x", "y").get();
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  private static class TestServer implements AutoCloseable {
    private final HttpServer server;

    TestServer(String context, HttpHandler handler) throws IOException {
      HttpHandler safeHandler = exchange -> {
        try {
          handler.handle(exchange);
        } catch (RuntimeException | Error e) {
          exchange.sendResponseHeaders(503, 0);
          throw e;
        }
      };
      server = HttpServer.create(new InetSocketAddress("localhost",0), 0);
      server.createContext(context, safeHandler);
      server.setExecutor(null);
      server.start();
    }

    TestServer(HttpHandler handler) throws IOException {
      this("/", handler);
    }

    @Override
    public void close() throws Exception {
      server.stop(0);
    }
  }

  public static class ExampleBean {
    private String field1;
    private int field2;

    public ExampleBean() {
    }

    public ExampleBean(String field1, int field2) {
      this.field1 = field1;
      this.field2 = field2;
    }

    public String getField1() {
      return field1;
    }

    public int getField2() {
      return field2;
    }

    public ExampleBean setField1(String field1) {
      this.field1 = field1;
      return this;
    }

    public ExampleBean setField2(int field2) {
      this.field2 = field2;
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ExampleBean that = (ExampleBean) o;
      return field2 == that.field2 && Objects.equals(field1, that.field1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2);
    }
  }
}
