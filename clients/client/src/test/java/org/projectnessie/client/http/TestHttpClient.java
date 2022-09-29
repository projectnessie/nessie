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
package org.projectnessie.client.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.projectnessie.client.util.HttpTestUtil.writeEmptyResponse;
import static org.projectnessie.client.util.HttpTestUtil.writeResponseBody;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.projectnessie.client.util.HttpTestServer;
import org.projectnessie.model.CommitMeta;

@Execution(ExecutionMode.CONCURRENT)
@ExtendWith(SoftAssertionsExtension.class)
public class TestHttpClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.now();

  @InjectSoftAssertions protected SoftAssertions soft;

  private static HttpRequest get(InetSocketAddress address) {
    return get(address, false);
  }

  private static HttpRequest get(InetSocketAddress address, boolean disableCompression) {
    return get(address, 15000, 15000, disableCompression);
  }

  private static HttpRequest get(
      InetSocketAddress address, int connectTimeout, int readTimeout, boolean disableCompression) {
    return HttpClient.builder()
        .setBaseUri(URI.create("http://localhost:" + address.getPort()))
        .setObjectMapper(MAPPER)
        .setConnectionTimeoutMillis(connectTimeout)
        .setReadTimeoutMillis(readTimeout)
        .setDisableCompression(disableCompression)
        .build()
        .newRequest();
  }

  @Test
  void testWriteWithVariousSizes() throws Exception {

    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          ArrayBean input;
          switch (req.getMethod()) {
            case "PUT":
            case "POST":
              try (InputStream in = req.getInputStream()) {
                input = MAPPER.readValue(in, ArrayBean.class);
              }
              break;
            case "GET":
            case "DELETE":
              input =
                  ArrayBean.construct(
                      Integer.parseInt(req.getParameter("len")),
                      Integer.parseInt(req.getParameter("num")));
              break;
            default:
              fail();
              return;
          }

          resp.addHeader("Content-Type", "application/json");
          resp.setStatus(200);

          try (OutputStream os = resp.getOutputStream()) {
            MAPPER.writeValue(os, input);
          }
        };

    try (HttpTestServer server = new HttpTestServer(handler)) {
      for (boolean disableCompression : new boolean[] {false, true}) {
        for (int num : new int[] {1, 10, 20, 100}) {
          int len = 10_000;

          ArrayBean inputBean = ArrayBean.construct(10_000, num);

          soft.assertThatCode(
                  () ->
                      assertThat(
                              get(server.getAddress(), disableCompression)
                                  .queryParam("len", Integer.toString(len))
                                  .queryParam("num", Integer.toString(num))
                                  .get()
                                  .readEntity(ArrayBean.class))
                          .isEqualTo(inputBean))
              .describedAs("GET, disableCompression:%s, num:%d", disableCompression, num)
              .isNull();
          soft.assertThatCode(
                  () ->
                      assertThat(
                              get(server.getAddress(), disableCompression)
                                  .queryParam("len", Integer.toString(len))
                                  .queryParam("num", Integer.toString(num))
                                  .delete()
                                  .readEntity(ArrayBean.class))
                          .isEqualTo(inputBean))
              .describedAs("DELETE, disableCompression:%s, num:%d", disableCompression, num)
              .isNull();
          soft.assertThatCode(
                  () ->
                      assertThat(
                              get(server.getAddress(), disableCompression)
                                  .put(inputBean)
                                  .readEntity(ArrayBean.class))
                          .isEqualTo(inputBean))
              .describedAs("PUT, disableCompression:%s, num:%d", disableCompression, num)
              .isNull();
          soft.assertThatCode(
                  () ->
                      assertThat(
                              get(server.getAddress(), disableCompression)
                                  .post(inputBean)
                                  .readEntity(ArrayBean.class))
                          .isEqualTo(inputBean))
              .describedAs("POST, disableCompression:%s, num:%d", disableCompression, num)
              .isNull();
        }
      }
    }
  }

  @Test
  void testGet() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          Assertions.assertEquals("GET", req.getMethod());
          String response = MAPPER.writeValueAsString(inputBean);
          writeResponseBody(resp, response);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      ExampleBean bean = get(server.getAddress()).get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    }
  }

  @Test
  void testReadTimeout() {
    HttpTestServer.RequestHandler handler = (req, resp) -> {};
    Assertions.assertThrows(
        HttpClientReadTimeoutException.class,
        () -> {
          try (HttpTestServer server = new HttpTestServer(handler)) {
            get(server.getAddress(), 15000, 1, true).get().readEntity(ExampleBean.class);
          }
        });
  }

  @Test
  void testPut() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          Assertions.assertEquals("PUT", req.getMethod());
          try (InputStream in = req.getInputStream()) {
            Object bean = MAPPER.readerFor(ExampleBean.class).readValue(in);
            Assertions.assertEquals(inputBean, bean);
          }
          writeEmptyResponse(resp);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      get(server.getAddress()).put(inputBean);
    }
  }

  @Test
  void testPost() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          Assertions.assertEquals("POST", req.getMethod());
          try (InputStream in = req.getInputStream()) {
            Object bean = MAPPER.readerFor(ExampleBean.class).readValue(in);
            Assertions.assertEquals(inputBean, bean);
          }
          writeEmptyResponse(resp);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      get(server.getAddress()).post(inputBean);
    }
  }

  @Test
  void testDelete() throws Exception {
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          Assertions.assertEquals("DELETE", req.getMethod());
          writeEmptyResponse(resp);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      get(server.getAddress()).delete();
    }
  }

  @Test
  void testGetQueryParam() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          Assertions.assertEquals("x=y", req.getQueryString());
          Assertions.assertEquals("GET", req.getMethod());
          String response = MAPPER.writeValueAsString(inputBean);
          writeResponseBody(resp, response);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      ExampleBean bean =
          get(server.getAddress()).queryParam("x", "y").get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    }
  }

  @Test
  void testGetMultipleQueryParam() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          String[] queryParams = req.getQueryString().split("&");
          Assertions.assertEquals(2, queryParams.length);
          Set<String> queryParamSet = new HashSet<>(Arrays.asList(queryParams));
          Assertions.assertTrue(queryParamSet.contains("x=y"));
          Assertions.assertTrue(queryParamSet.contains("a=b"));
          Assertions.assertEquals("GET", req.getMethod());
          String response = MAPPER.writeValueAsString(inputBean);
          writeResponseBody(resp, response);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      ExampleBean bean =
          get(server.getAddress())
              .queryParam("x", "y")
              .queryParam("a", "b")
              .get()
              .readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    }
  }

  @Test
  void testGetNullQueryParam() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          String queryParams = req.getQueryString();
          Assertions.assertNull(queryParams);
          Assertions.assertEquals("GET", req.getMethod());
          String response = MAPPER.writeValueAsString(inputBean);
          writeResponseBody(resp, response);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      ExampleBean bean =
          get(server.getAddress())
              .queryParam("x", (String) null)
              .get()
              .readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    }
  }

  @Test
  void testGetTemplate() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          Assertions.assertEquals("GET", req.getMethod());
          String response = MAPPER.writeValueAsString(inputBean);
          writeResponseBody(resp, response);
        };
    try (HttpTestServer server = new HttpTestServer("/a/b", handler)) {
      ExampleBean bean = get(server.getAddress()).path("a/b").get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
      bean =
          get(server.getAddress())
              .path("a/{b}")
              .resolveTemplate("b", "b")
              .get()
              .readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    }
  }

  @Test
  void testGetTemplateThrows() throws Exception {
    HttpTestServer.RequestHandler handler = (req, resp) -> fail();
    try (HttpTestServer server = new HttpTestServer("/a/b", handler)) {
      Assertions.assertThrows(
          HttpClientException.class,
          () -> get(server.getAddress()).path("a/{b}").get().readEntity(ExampleBean.class));
      Assertions.assertThrows(
          HttpClientException.class,
          () ->
              get(server.getAddress())
                  .path("a/b")
                  .resolveTemplate("b", "b")
                  .get()
                  .readEntity(ExampleBean.class));
    }
  }

  @Test
  void testFilters() throws Exception {
    AtomicBoolean requestFilterCalled = new AtomicBoolean(false);
    AtomicBoolean responseFilterCalled = new AtomicBoolean(false);
    AtomicReference<ResponseContext> responseContextGotCallback = new AtomicReference<>();
    AtomicReference<ResponseContext> responseContextGotFilter = new AtomicReference<>();
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          assertThat(req.getHeader("x")).isEqualTo("y");
          writeEmptyResponse(resp);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      HttpClient.Builder client =
          HttpClient.builder()
              .setBaseUri(URI.create("http://localhost:" + server.getAddress().getPort()))
              .setObjectMapper(MAPPER);
      client.addRequestFilter(
          context -> {
            requestFilterCalled.set(true);
            context.putHeader("x", "y");
            context.addResponseCallback(
                (responseContext, failure) -> {
                  responseContextGotCallback.set(responseContext);
                  Assertions.assertNull(failure);
                });
          });
      client.addResponseFilter(
          con -> {
            try {
              Assertions.assertEquals(Status.OK, con.getResponseCode());
              responseFilterCalled.set(true);
              responseContextGotFilter.set(con);
            } catch (IOException e) {
              throw new IOError(e);
            }
          });
      client.build().newRequest().get();
      Assertions.assertNotNull(responseContextGotFilter.get());
      Assertions.assertSame(responseContextGotFilter.get(), responseContextGotCallback.get());
      Assertions.assertTrue(responseFilterCalled.get());
      Assertions.assertTrue(requestFilterCalled.get());
    }
  }

  @Test
  void testHeaders() throws Exception {
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          assertThat(req.getHeader("x")).isEqualTo("y");
          writeEmptyResponse(resp);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      get(server.getAddress()).header("x", "y").get();
    }
  }

  @Test
  void testMultiValueHeaders() throws Exception {
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          List<String> values = new ArrayList<>();
          for (Enumeration<String> e = req.getHeaders("x"); e != null && e.hasMoreElements(); ) {
            values.add(e.nextElement());
          }
          assertThat(values).containsExactly("y", "z");
          writeEmptyResponse(resp);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      get(server.getAddress()).header("x", "y").header("x", "z").get();
    }
  }

  @SuppressWarnings("unused")
  public static class ExampleBean {
    private String field1;
    private int field2;
    private Instant field3;

    public ExampleBean() {}

    /** all-args constructor. */
    public ExampleBean(String field1, int field2, Instant field3) {
      this.field1 = field1;
      this.field2 = field2;
      this.field3 = field3;
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

    @JsonSerialize(using = CommitMeta.InstantSerializer.class)
    @JsonDeserialize(using = CommitMeta.InstantDeserializer.class)
    public Instant getField3() {
      return field3;
    }

    public void setField3(Instant field3) {
      this.field3 = field3;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ExampleBean)) {
        return false;
      }
      ExampleBean that = (ExampleBean) o;
      return field2 == that.field2
          && Objects.equals(field1, that.field1)
          && Objects.equals(field3, that.field3);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2, field3);
    }
  }

  @SuppressWarnings("unused")
  public static class ArrayBean {
    private List<String> data;

    public static ArrayBean construct(int elementLength, int numElements) {
      List<String> data =
          IntStream.range(0, numElements)
              .mapToObj(
                  e -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append(e).append(':');
                    while (sb.length() < elementLength) {
                      sb.append(e);
                    }
                    return sb.toString();
                  })
              .collect(Collectors.toList());

      ArrayBean bean = new ArrayBean();
      bean.data = data;
      return bean;
    }

    public ArrayBean() {}

    public List<String> getData() {
      return data;
    }

    public void setData(List<String> data) {
      this.data = data;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ArrayBean)) {
        return false;
      }
      ArrayBean arrayBean = (ArrayBean) o;
      return data.equals(arrayBean.data);
    }

    @Override
    public int hashCode() {
      return Objects.hash(data);
    }
  }
}
