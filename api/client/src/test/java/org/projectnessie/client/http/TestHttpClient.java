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
import static org.assertj.core.api.Assumptions.assumeThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
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
import com.google.common.base.Splitter;
import java.io.BufferedReader;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;
import org.projectnessie.client.http.impl.jdk8.UrlConnectionClient;
import org.projectnessie.client.util.HttpTestServer;
import org.projectnessie.model.CommitMeta;

@ExtendWith(SoftAssertionsExtension.class)
public class TestHttpClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.now();

  @InjectSoftAssertions protected SoftAssertions soft;

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
    try (HttpTestServer server = new HttpTestServer("/", handler, ssl);
        HttpClient client =
            createClient(
                server.getUri(),
                b -> {
                  if (ssl) {
                    b.setSslContext(server.getSslContext());
                  }
                  b.setHttp2Upgrade(http2);
                })) {

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
    try (HttpTestServer server = new HttpTestServer("/", (req, resp) -> {}, true);
        HttpClient insecureClient =
            HttpClient.builder().setBaseUri(server.getUri()).setObjectMapper(MAPPER).build()) {
      HttpRequest request;
      request = insecureClient.newRequest();
      assertThatThrownBy(request::get)
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
              resp.sendError(500);
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
        try (HttpClient client =
            createClient(
                server.getUri(),
                b -> {
                  b.setDisableCompression(disableCompression).setHttp2Upgrade(http2);
                  if (ssl) {
                    b.setSslContext(server.getSslContext());
                  }
                })) {

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
  }

  @Test
  void testGet() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          soft.assertThat(req.getMethod()).isEqualTo("GET");
          String response = MAPPER.writeValueAsString(inputBean);
          writeResponseBody(resp, response);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      URI baseUri = server.getUri();
      try (HttpClient client = createClient(baseUri, b -> {})) {
        ExampleBean bean = client.newRequest().get().readEntity(ExampleBean.class);
        soft.assertThat(bean).isEqualTo(inputBean);
      }
    }
  }

  @Test
  void testPerRequestBaseUri() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          soft.assertThat(req.getMethod()).isEqualTo("GET");
          String response = MAPPER.writeValueAsString(inputBean);
          writeResponseBody(resp, response);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      try (HttpClient client = createClient(null, b -> {})) {
        ExampleBean bean = client.newRequest(server.getUri()).get().readEntity(ExampleBean.class);
        soft.assertThat(bean).isEqualTo(inputBean);
      }
    }
  }

  @Test
  void testNullPerRequestBaseUri() {
    try (HttpClient client = createClient(null, b -> {})) {
      assertThatThrownBy(() -> client.newRequest(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessage("Base URI cannot be null");
    }
  }

  @Test
  void testInvalidPerRequestBaseUri() {
    try (HttpClient client = createClient(null, b -> {})) {
      assertThatThrownBy(() -> client.newRequest(URI.create("file:///foo/bar/baz")))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Base URI must be a valid http or https address");
    }
  }

  @Test
  void testPut() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          soft.assertThat(req.getMethod()).isEqualTo("PUT");
          try (InputStream in = req.getInputStream()) {
            Object bean = MAPPER.readerFor(ExampleBean.class).readValue(in);
            soft.assertThat(bean).isEqualTo(inputBean);
          }
          writeEmptyResponse(resp);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      URI baseUri = server.getUri();
      try (HttpClient client = createClient(baseUri, b -> {})) {
        client.newRequest().put(inputBean);
      }
    }
  }

  @Test
  void testPost() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          soft.assertThat(req.getMethod()).isEqualTo("POST");
          try (InputStream in = req.getInputStream()) {
            Object bean = MAPPER.readerFor(ExampleBean.class).readValue(in);
            soft.assertThat(bean).isEqualTo(inputBean);
          }
          writeEmptyResponse(resp);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      URI baseUri = server.getUri();
      try (HttpClient client = createClient(baseUri, b -> {})) {
        client.newRequest().post(inputBean);
      }
    }
  }

  @Test
  void testDelete() throws Exception {
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          soft.assertThat(req.getMethod()).isEqualTo("DELETE");
          writeEmptyResponse(resp);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      URI baseUri = server.getUri();
      try (HttpClient client = createClient(baseUri, b -> {})) {
        client.newRequest().delete();
      }
    }
  }

  @Test
  void testGetQueryParam() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          soft.assertThat(req.getQueryString()).isEqualTo("x=y");
          soft.assertThat(req.getMethod()).isEqualTo("GET");
          String response = MAPPER.writeValueAsString(inputBean);
          writeResponseBody(resp, response);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      URI baseUri = server.getUri();
      try (HttpClient client = createClient(baseUri, b -> {})) {
        ExampleBean bean =
            client.newRequest().queryParam("x", "y").get().readEntity(ExampleBean.class);
        soft.assertThat(bean).isEqualTo(inputBean);
      }
    }
  }

  @Test
  void testGetMultipleQueryParam() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          String[] queryParams = req.getQueryString().split("&");
          soft.assertThat(queryParams).hasSize(2);
          Set<String> queryParamSet = new HashSet<>(Arrays.asList(queryParams));
          assertThat(queryParamSet).contains("x=y", "a=b");
          soft.assertThat(req.getMethod()).isEqualTo("GET");
          String response = MAPPER.writeValueAsString(inputBean);
          writeResponseBody(resp, response);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      URI baseUri = server.getUri();
      try (HttpClient client = createClient(baseUri, b -> {})) {
        ExampleBean bean =
            client
                .newRequest()
                .queryParam("x", "y")
                .queryParam("a", "b")
                .get()
                .readEntity(ExampleBean.class);
        soft.assertThat(bean).isEqualTo(inputBean);
      }
    }
  }

  @Test
  void testGetNullQueryParam() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          String queryParams = req.getQueryString();
          soft.assertThat(queryParams).isNull();
          soft.assertThat(req.getMethod()).isEqualTo("GET");
          String response = MAPPER.writeValueAsString(inputBean);
          writeResponseBody(resp, response);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      URI baseUri = server.getUri();
      try (HttpClient client = createClient(baseUri, b -> {})) {
        ExampleBean bean =
            client.newRequest().queryParam("x", (String) null).get().readEntity(ExampleBean.class);
        soft.assertThat(bean).isEqualTo(inputBean);
      }
    }
  }

  @Test
  void testGetTemplate() throws Exception {
    ExampleBean inputBean = new ExampleBean("x", 1, NOW);
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          soft.assertThat(req.getMethod()).isEqualTo("GET");
          String response = MAPPER.writeValueAsString(inputBean);
          writeResponseBody(resp, response);
        };
    try (HttpTestServer server = new HttpTestServer("/a/b", handler)) {
      URI baseUri = server.getUri();
      try (HttpClient client = createClient(baseUri, b1 -> {})) {
        ExampleBean bean = client.newRequest().path("a/b").get().readEntity(ExampleBean.class);
        soft.assertThat(bean).isEqualTo(inputBean);
        bean =
            client
                .newRequest()
                .path("a/{b}")
                .resolveTemplate("b", "b")
                .get()
                .readEntity(ExampleBean.class);
        soft.assertThat(bean).isEqualTo(inputBean);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Status.class)
  void testHttpResponses(Status status) throws Exception {
    // HTTP/304 defines that no response body must be sent
    assumeThat(status).isNotEqualTo(Status.NOT_MODIFIED);

    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          resp.setStatus(status.getCode());
          resp.setContentType("application/json");
          try (OutputStream os = resp.getOutputStream()) {
            MAPPER.writeValue(os, new ExampleBean());
          }
        };
    try (HttpTestServer server = new HttpTestServer("/a/b", handler)) {
      AtomicReference<ResponseContext> responseContext = new AtomicReference<>();
      AbstractThrowableAssert<?, ? extends Throwable> reqAssert;
      try (HttpClient client =
          createClient(server.getUri(), b -> b.addResponseFilter(responseContext::set))) {
        reqAssert =
            soft.assertThatCode(() -> client.newRequest().get().readEntity(ExampleBean.class));
      }
      if (status.getCode() < 400) {
        reqAssert.doesNotThrowAnyException();
      } else {
        reqAssert.isInstanceOf(HttpClientException.class);
      }
      soft.assertThat(responseContext.get())
          .isNotNull()
          .extracting(
              rc -> {
                try {
                  return rc.getResponseCode();
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              })
          .isEqualTo(status);
    }
  }

  @Test
  void testGetTemplateThrows() throws Exception {
    HttpTestServer.RequestHandler handler =
        (req, resp) -> resp.sendError(Status.INTERNAL_SERVER_ERROR.getCode());
    try (HttpTestServer server = new HttpTestServer("/a/b", handler)) {
      AtomicReference<ResponseContext> responseContext = new AtomicReference<>();
      try (HttpClient client =
          createClient(server.getUri(), b -> b.addResponseFilter(responseContext::set))) {

        soft.assertThatThrownBy(
                () -> client.newRequest().path("a/{b}").get().readEntity(ExampleBean.class))
            .isInstanceOf(HttpClientException.class);
        soft.assertThat(responseContext.get())
            .isNotNull()
            .extracting(
                rc -> {
                  try {
                    return rc.getResponseCode();
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
            .isEqualTo(Status.INTERNAL_SERVER_ERROR);

        responseContext.set(null);
        soft.assertThatThrownBy(
                () ->
                    client
                        .newRequest()
                        .path("a/b")
                        .resolveTemplate("b", "b")
                        .get()
                        .readEntity(ExampleBean.class))
            .isInstanceOf(HttpClientException.class)
            .hasMessageContaining(
                "Cannot build uri. Not all template keys (b) were used in uri a/b");
      }
      soft.assertThat(responseContext.get()).isNull();
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
      HttpClient.Builder builder =
          HttpClient.builder().setBaseUri(server.getUri()).setObjectMapper(MAPPER);
      builder.addRequestFilter(
          context -> {
            requestFilterCalled.set(true);
            context.putHeader("x", "y");
            context.addResponseCallback(
                (responseContext, failure) -> {
                  responseContextGotCallback.set(responseContext);
                  soft.assertThat(failure).isNull();
                });
          });
      builder.addResponseFilter(
          con -> {
            try {
              soft.assertThat(con.getResponseCode()).isEqualTo(Status.OK);
              responseFilterCalled.set(true);
              responseContextGotFilter.set(con);
            } catch (IOException e) {
              throw new IOError(e);
            }
          });
      try (HttpClient client = builder.build()) {
        client.newRequest().get();
      }
      soft.assertThat(responseContextGotFilter.get())
          .isNotNull()
          .isSameAs(responseContextGotCallback.get());
      soft.assertThat(responseFilterCalled.get()).isTrue();
      soft.assertThat(requestFilterCalled.get()).isTrue();
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
      URI baseUri = server.getUri();
      try (HttpClient client = createClient(baseUri, b -> {})) {
        client.newRequest().header("x", "y").get();
      }
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
      URI baseUri = server.getUri();
      try (HttpClient client = createClient(baseUri, b -> {})) {
        client.newRequest().header("x", "y").header("x", "z").get();
      }
    }
  }

  @ParameterizedTest
  @MethodSource
  void testPostForm(ExampleBean inputBean) throws Exception {
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          soft.assertThat(req.getMethod()).isEqualTo("POST");
          soft.assertThat(req.getContentType()).isEqualTo("application/x-www-form-urlencoded");
          Map<String, String> data = decodeFormData(req.getInputStream());
          ExampleBean actual = MAPPER.convertValue(data, ExampleBean.class);
          soft.assertThat(actual).isEqualTo(inputBean);
          writeEmptyResponse(resp);
        };
    try (HttpTestServer server = new HttpTestServer(handler)) {
      URI baseUri = server.getUri();
      try (HttpClient client = createClient(baseUri, b -> {})) {
        client.newRequest().postForm(inputBean);
      }
    }
  }

  static Stream<ExampleBean> testPostForm() {
    return Stream.of(new ExampleBean(), new ExampleBean("x", 1, NOW));
  }

  @Test
  void testCloseJava11Client() throws Exception {
    assumeThatCode(() -> Class.forName("java.net.http.HttpClient")).doesNotThrowAnyException();
    HttpRuntimeConfig config = mock(HttpRuntimeConfig.class);
    when(config.getConnectionTimeoutMillis()).thenReturn(100);
    HttpClient client =
        Class.forName("org.projectnessie.client.http.impl.jdk11.JavaHttpClient")
            .asSubclass(HttpClient.class)
            .getConstructor(HttpRuntimeConfig.class)
            .newInstance(config);
    client.close();
    verify(config).close();
  }

  @Test
  void testCloseJava8Client() {
    HttpRuntimeConfig config = mock(HttpRuntimeConfig.class);
    when(config.getConnectionTimeoutMillis()).thenReturn(100);
    HttpClient client = new UrlConnectionClient(config);
    client.close();
    verify(config).close();
  }

  @Test
  void testCloseHttpRuntimeConfig() throws Exception {
    HttpAuthentication authentication = mock(HttpAuthentication.class);
    HttpRuntimeConfig config =
        HttpRuntimeConfig.builder()
            .baseUri(URI.create("http://localhost:19120"))
            .mapper(MAPPER)
            .responseFactory((ctx, mapper) -> null)
            .readTimeoutMillis(100)
            .connectionTimeoutMillis(100)
            .isDisableCompression(false)
            .sslContext(SSLContext.getDefault())
            .authentication(authentication)
            .build();
    config.close();
    verify(authentication).close();
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

  public static Map<String, String> decodeFormData(InputStream in) throws IOException {
    Map<String, String> decodedValues;
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String data = reader.readLine();
      String decodedData = URLDecoder.decode(data, StandardCharsets.UTF_8.name());
      decodedValues = new HashMap<>();
      Iterable<String> keyValues = Splitter.on('&').split(decodedData);
      for (String keyValue : keyValues) {
        List<String> parts = Splitter.on('=').splitToList(keyValue);
        String key = parts.get(0);
        String value = parts.get(1);
        decodedValues.put(key, value);
      }
    }
    return decodedValues;
  }
}
