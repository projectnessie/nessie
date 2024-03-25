/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.objectstoragemock;

import com.fasterxml.jackson.jakarta.rs.xml.JacksonXMLProvider;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseFilter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public abstract class ObjectStorageMock {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageMock.class);

  @Value.Default
  public String initAddress() {
    return "127.0.0.1";
  }

  public static ImmutableObjectStorageMock.Builder builder() {
    return ImmutableObjectStorageMock.builder();
  }

  public abstract Map<String, Bucket> buckets();

  public interface MockServer extends AutoCloseable {
    URI getS3BaseUri();

    URI getGcsBaseUri();

    URI getAdlsGen2BaseUri();

    default Map<String, String> icebergProperties() {
      Map<String, String> props = new HashMap<>();
      props.put("s3.access-key-id", "accessKey");
      props.put("s3.secret-access-key", "secretKey");
      props.put("s3.endpoint", getS3BaseUri().toString());
      props.put("http-client.type", "urlconnection");
      return props;
    }
  }

  private static final class MockServerImpl implements MockServer {

    private final Server server;

    private final URI baseUri;

    public MockServerImpl(URI initUri, ResourceConfig config) {
      this.server = JettyHttpContainerFactory.createServer(initUri, config, true);
      customizeUriCompliance();
      this.baseUri = baseUri(server, initUri);
    }

    /**
     * Allows ambiguous path separators, because Microsoft's Azure clients do send URL-encoded path
     * separators, so {@code /} as {@code %2f}, which are considered to be insecure and is (should
     * be) rejected by containers since <a
     * href="https://github.com/jakartaee/servlet/blob/6.0.0-RELEASE/spec/src/main/asciidoc/servlet-spec-body.adoc#352-uri-path-canonicalization">service
     * spec v6</a>.
     */
    private void customizeUriCompliance() {
      for (Connector connector : server.getConnectors()) {
        connector.getConnectionFactories().stream()
            .filter(factory -> factory instanceof HttpConnectionFactory)
            .forEach(
                factory -> {
                  HttpConfiguration httpConfig =
                      ((HttpConnectionFactory) factory).getHttpConfiguration();
                  httpConfig.setUriCompliance(
                      UriCompliance.from(Set.of(UriCompliance.Violation.AMBIGUOUS_PATH_SEPARATOR)));
                });
      }
    }

    private static URI baseUri(Server server, URI initUri) {
      for (Connector connector : server.getConnectors()) {
        if (connector instanceof ServerConnector) {
          ServerConnector sc = (ServerConnector) connector;
          int localPort = sc.getLocalPort();
          try {
            return new URI(
                initUri.getScheme(),
                initUri.getUserInfo(),
                initUri.getHost(),
                localPort,
                initUri.getPath(),
                null,
                null);
          } catch (URISyntaxException e) {
            throw new RuntimeException(e);
          }
        }
      }

      throw new IllegalArgumentException("Server has no connectors");
    }

    @Override
    public URI getS3BaseUri() {
      return baseUri.resolve("s3/");
    }

    @Override
    public URI getGcsBaseUri() {
      return baseUri;
    }

    @Override
    public URI getAdlsGen2BaseUri() {
      return baseUri.resolve("adlsgen2/");
    }

    @Override
    public void close() throws Exception {
      if (server != null) {
        server.stop();
      }
    }
  }

  public MockServer start() {
    ResourceConfig config = new ResourceConfig();

    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            this.bind(ObjectStorageMock.this).to(ObjectStorageMock.class);
          }
        });
    config.register(JacksonXMLProvider.class);

    config.register(S3Resource.class);
    config.register(AdlsGen2Resource.class);
    config.register(GcsResource.class);

    if (LOGGER.isDebugEnabled()) {
      config.register(
          (ContainerRequestFilter)
              requestContext -> {
                LOGGER.debug(
                    "{} {} {}",
                    requestContext.getMethod(),
                    requestContext.getUriInfo().getPath(),
                    requestContext.getUriInfo().getRequestUri().getQuery());
                requestContext.getHeaders().forEach((k, v) -> LOGGER.debug("  {}: {}", k, v));
              });
      config.register(
          (ContainerResponseFilter)
              (requestContext, responseContext) -> {
                LOGGER.debug("{}", responseContext.getStatusInfo());
                responseContext.getHeaders().forEach((k, v) -> LOGGER.debug("  {}: {}", k, v));
              });
    }

    URI initUri = URI.create(String.format("http://%s:0/", initAddress()));

    return new MockServerImpl(initUri, config);
  }
}
