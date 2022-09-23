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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.projectnessie.client.util.HttpTestUtil.writeEmptyResponse;
import static org.projectnessie.client.util.HttpTestUtil.writeResponseBody;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.net.ssl.SSLHandshakeException;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.client.util.HttpTestServer;
import org.projectnessie.model.CommitMeta;

@Execution(ExecutionMode.CONCURRENT)
@ExtendWith(SoftAssertionsExtension.class)
public class TestHttpClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.now();

  @InjectSoftAssertions protected SoftAssertions soft;

  private HttpRequest get(URI baseUri) {
    return createClient(baseUri, b -> {}).newRequest();
  }

  private HttpClient createClient(URI baseUri, Consumer<HttpClient.Builder> customizer) {
    HttpClient.Builder b =
        HttpClient.builder()
            .setBaseUri(baseUri)
            .setObjectMapper(MAPPER)
            .setConnectionTimeoutMillis(15000)
            .setReadTimeoutMillis(15000);
    customizer.accept(b);
    return b.build();
  }

  @ParameterizedTest
  @CsvSource({"false, false", "false, true", "true, false", "true, true"})
  void testHttpCombinations(boolean ssl, boolean http2) throws Exception {
    if (http2) {
      // Old URLConnection based client cannot handle HTTP/2
      assumeThat(JRE.currentVersion()).matches(jre -> jre.ordinal() >= JRE.JAVA_11.ordinal());
    }

    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          ObjectNode r = new ObjectNode(JsonNodeFactory.instance);
          r.put("secure", req.isSecure());
          r.put("protocol", req.getProtocol());
          r.put("method", req.getMethod());
          r.put("scheme", req.getScheme());

          resp.setContentType("application/json");
          try (OutputStream os = resp.getOutputStream()) {
            MAPPER.writeValue(os, r);
          }
        };
    try (HttpTestServer server = new HttpTestServer("/", handler, ssl)) {
      HttpClient client =
          createClient(
              server.getUri(),
              b -> {
                if (ssl) {
                  b.setSslContext(server.getSslContext());
                }
                b.setHttp2Upgrade(http2);
              });

      JsonNode result = client.newRequest().get().readEntity(JsonNode.class);
      soft.assertThat(result)
          .containsExactly(
              BooleanNode.valueOf(ssl),
              TextNode.valueOf(http2 ? "HTTP/2.0" : "HTTP/1.1"),
              TextNode.valueOf("GET"),
              TextNode.valueOf(ssl ? "https" : "http"));
    }
  }

  @Test
  void testHttpsWithInsecureClient() throws Exception {
    try (HttpTestServer server = new HttpTestServer("/", (req, resp) -> {}, true)) {
      HttpRequest insecureClient =
          HttpClient.builder()
              .setBaseUri(server.getUri())
              .setObjectMapper(MAPPER)
              .build()
              .newRequest();
      assertThatThrownBy(insecureClient::get)
          .isInstanceOf(HttpClientException.class)
          .cause()
          .satisfiesAnyOf(
              t -> assertThat(t).isInstanceOf(SSLHandshakeException.class),
              t -> assertThat(t.getCause()).isInstanceOf(SSLHandshakeException.class));
    }
  }

  @ParameterizedTest
  @CsvSource({"false, false", "false, true", "true, false", "true, true"})
  void testWriteWithVariousSizes(boolean ssl, boolean http2) throws Exception {
    if (http2) {
      // Old URLConnection based client cannot handle HTTP/2
      assumeThat(JRE.currentVersion()).matches(jre -> jre.ordinal() >= JRE.JAVA_11.ordinal());
    }

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

    try (HttpTestServer server = new HttpTestServer(handler, ssl)) {
      for (boolean disableCompression : new boolean[] {false, true}) {
        HttpClient client =
            createClient(
                server.getUri(),
                b -> {
                  b.setDisableCompression(disableCompression).setHttp2Upgrade(http2);
                  if (ssl) {
                    b.setSslContext(server.getSslContext());
                  }
                });

        // Intentionally repeat a bunch of requests as fast as possible to validate that the
        // server/client combination works fine.
        for (int i = 0; i < 5; i++) {
          for (int num : new int[] {1, 10, 20, 100}) {
            int len = 10_000;

            ArrayBean inputBean = ArrayBean.construct(10_000, num);

            Supplier<HttpRequest> newRequest =
                () ->
                    client
                        .newRequest()
                        .queryParam("len", Integer.toString(len))
                        .queryParam("num", Integer.toString(num))
                        .queryParam("disableCompression", Boolean.toString(disableCompression));

            soft.assertThatCode(
                    () ->
                        assertThat(newRequest.get().get().readEntity(ArrayBean.class))
                            .isEqualTo(inputBean))
                .describedAs("GET, disableCompression:%s, num:%d", disableCompression, num)
                .doesNotThrowAnyException();

            soft.assertThatCode(
                    () ->
                        assertThat(newRequest.get().delete().readEntity(ArrayBean.class))
                            .isEqualTo(inputBean))
                .describedAs("DELETE, disableCompression:%s, num:%d", disableCompression, num)
                .doesNotThrowAnyException();

            soft.assertThatCode(
                    () ->
                        assertThat(newRequest.get().put(inputBean).readEntity(ArrayBean.class))
                            .isEqualTo(inputBean))
                .describedAs("PUT, disableCompression:%s, num:%d", disableCompression, num)
                .doesNotThrowAnyException();

            soft.assertThatCode(
                    () ->
                        assertThat(newRequest.get().post(inputBean).readEntity(ArrayBean.class))
                            .isEqualTo(inputBean))
                .describedAs("POST, disableCompression:%s, num:%d", disableCompression, num)
                .doesNotThrowAnyException();
          }
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
      ExampleBean bean = get(server.getUri()).get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
    }
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
      get(server.getUri()).put(inputBean);
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
      get(server.getUri()).post(inputBean);
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
      get(server.getUri()).delete();
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
          get(server.getUri()).queryParam("x", "y").get().readEntity(ExampleBean.class);
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
          get(server.getUri())
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
          get(server.getUri()).queryParam("x", (String) null).get().readEntity(ExampleBean.class);
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
      ExampleBean bean = get(server.getUri()).path("a/b").get().readEntity(ExampleBean.class);
      Assertions.assertEquals(inputBean, bean);
      bean =
          get(server.getUri())
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
          () -> get(server.getUri()).path("a/{b}").get().readEntity(ExampleBean.class));
      Assertions.assertThrows(
          HttpClientException.class,
          () ->
              get(server.getUri())
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
          HttpClient.builder().setBaseUri(server.getUri()).setObjectMapper(MAPPER);
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
      get(server.getUri()).header("x", "y").get();
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
      get(server.getUri()).header("x", "y").header("x", "z").get();
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
