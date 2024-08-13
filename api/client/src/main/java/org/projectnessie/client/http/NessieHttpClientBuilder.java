/*
 * Copyright (C) 2023 Dremio
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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_CLIENT_NAME;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_HTTP_2;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_HTTP_REDIRECT;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.util.function.Function;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.config.NessieClientConfigSources;

/** HTTP specific Nessie client builder interface. */
public interface NessieHttpClientBuilder extends NessieClientBuilder {

  /**
   * Whether to allow HTTP/2 upgrade, default is {@code false}.
   *
   * <p>Only valid on Java 11 and newer with Java's new HTTP client.
   */
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withHttp2Upgrade(boolean http2Upgrade);

  /**
   * Whether HTTP redirects are followed, default is to not follow redirects.
   *
   * <p>Valid values are the enum names of {@link java.net.http.HttpClient.Redirect}.
   *
   * <p>Only valid on Java 11 and newer with Java's new HTTP client.
   */
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withFollowRedirects(String redirects);

  /**
   * Whether to force using the {@link java.net.URLConnection} based client.
   *
   * @deprecated use {@link #withClientName(String)} with the name {@code URLConnection} instead.
   */
  @Deprecated
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withForceUrlConnectionClient(boolean forceUrlConnectionClient);

  @CanIgnoreReturnValue
  NessieHttpClientBuilder withClientName(String clientName);

  @CanIgnoreReturnValue
  NessieHttpClientBuilder withResponseFactory(HttpResponseFactory responseFactory);

  /**
   * Add a request filter to the client. Enables low-level access to the request/response
   * processing.
   */
  @CanIgnoreReturnValue
  NessieHttpClientBuilder addRequestFilter(RequestFilter filter);

  /**
   * Add a response filter to the client. Enables low-level access to the request/response
   * processing.
   */
  @CanIgnoreReturnValue
  NessieHttpClientBuilder addResponseFilter(ResponseFilter filter);

  @Override
  @CanIgnoreReturnValue
  NessieHttpClientBuilder fromConfig(Function<String, String> configuration);

  @Override
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withApiCompatibilityCheck(boolean enable);

  @Override
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withAuthenticationFromConfig(Function<String, String> configuration);

  @Override
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withAuthentication(NessieAuthentication authentication);

  @Override
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withTracing(boolean tracing);

  @Override
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withUri(URI uri);

  @Override
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withUri(String uri);

  @Override
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withReadTimeout(int readTimeoutMillis);

  @Override
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withConnectionTimeout(int connectionTimeoutMillis);

  @Override
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withDisableCompression(boolean disableCompression);

  @Override
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withSSLContext(SSLContext sslContext);

  @Override
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withSSLParameters(SSLParameters sslParameters);

  @CanIgnoreReturnValue
  NessieHttpClientBuilder withHttpHeader(String header, String value);

  /** Convenience base class for implementations of {@link NessieHttpClientBuilder}. */
  abstract class AbstractNessieHttpClientBuilder
      extends NessieClientBuilder.AbstractNessieClientBuilder implements NessieHttpClientBuilder {

    /**
     * Configure this HttpClientBuilder instance using a configuration object and standard Nessie
     * configuration keys defined by the constants defined in {@link NessieConfigConstants}.
     * Non-{@code null} values returned by the {@code configuration}-function will override
     * previously configured values.
     *
     * @param configuration The function that returns a configuration value for a configuration key.
     * @return {@code this}
     * @see NessieClientConfigSources
     */
    @Override
    public NessieHttpClientBuilder fromConfig(Function<String, String> configuration) {
      super.fromConfig(configuration);

      String s = configuration.apply(CONF_NESSIE_HTTP_2);
      if (s != null) {
        withHttp2Upgrade(Boolean.parseBoolean(s.trim()));
      }

      s = configuration.apply(CONF_NESSIE_HTTP_REDIRECT);
      if (s != null) {
        withFollowRedirects(s.trim());
      }

      @SuppressWarnings("deprecation")
      String forceUrlConnectionClient = NessieConfigConstants.CONF_FORCE_URL_CONNECTION_CLIENT;
      s = configuration.apply(forceUrlConnectionClient);
      if (s != null) {
        withForceUrlConnectionClient(Boolean.parseBoolean(s.trim()));
      }
      s = configuration.apply(CONF_NESSIE_CLIENT_NAME);
      if (s != null) {
        withClientName(s.trim());
      }

      return this;
    }

    @Override
    public NessieHttpClientBuilder withHttp2Upgrade(boolean http2Upgrade) {
      return this;
    }

    @Override
    public NessieHttpClientBuilder withFollowRedirects(String redirects) {
      return this;
    }

    @Override
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    public NessieHttpClientBuilder withForceUrlConnectionClient(boolean forceUrlConnectionClient) {
      return this;
    }

    @Override
    public NessieHttpClientBuilder withClientName(String clientName) {
      return this;
    }

    @Override
    public NessieHttpClientBuilder withResponseFactory(HttpResponseFactory responseFactory) {
      return this;
    }

    @Override
    public NessieHttpClientBuilder addRequestFilter(RequestFilter filter) {
      return this;
    }

    @Override
    public NessieHttpClientBuilder addResponseFilter(ResponseFilter filter) {
      return this;
    }

    @Override
    public NessieHttpClientBuilder withAuthenticationFromConfig(
        Function<String, String> configuration) {
      return (NessieHttpClientBuilder) super.withAuthenticationFromConfig(configuration);
    }

    @Override
    public NessieHttpClientBuilder withUri(String uri) {
      return (NessieHttpClientBuilder) super.withUri(uri);
    }

    @Override
    public NessieHttpClientBuilder withApiCompatibilityCheck(boolean enable) {
      return (NessieHttpClientBuilder) super.withApiCompatibilityCheck(enable);
    }

    @Override
    public NessieHttpClientBuilder withAuthentication(NessieAuthentication authentication) {
      return (NessieHttpClientBuilder) super.withAuthentication(authentication);
    }

    @Override
    public NessieHttpClientBuilder withTracing(boolean tracing) {
      return (NessieHttpClientBuilder) super.withTracing(tracing);
    }

    @Override
    public NessieHttpClientBuilder withUri(URI uri) {
      return (NessieHttpClientBuilder) super.withUri(uri);
    }

    @Override
    public NessieHttpClientBuilder withReadTimeout(int readTimeoutMillis) {
      return (NessieHttpClientBuilder) super.withReadTimeout(readTimeoutMillis);
    }

    @Override
    public NessieHttpClientBuilder withConnectionTimeout(int connectionTimeoutMillis) {
      return (NessieHttpClientBuilder) super.withConnectionTimeout(connectionTimeoutMillis);
    }

    @Override
    public NessieHttpClientBuilder withDisableCompression(boolean disableCompression) {
      return (NessieHttpClientBuilder) super.withDisableCompression(disableCompression);
    }

    @Override
    public NessieHttpClientBuilder withSSLCertificateVerificationDisabled(
        boolean certificateVerificationDisabled) {
      return (NessieHttpClientBuilder)
          super.withSSLCertificateVerificationDisabled(certificateVerificationDisabled);
    }

    @Override
    public NessieHttpClientBuilder withSSLContext(SSLContext sslContext) {
      return (NessieHttpClientBuilder) super.withSSLContext(sslContext);
    }

    @Override
    public NessieHttpClientBuilder withSSLParameters(SSLParameters sslParameters) {
      return (NessieHttpClientBuilder) super.withSSLParameters(sslParameters);
    }

    @Override
    public NessieHttpClientBuilder withHttpHeader(String header, String value) {
      return (NessieHttpClientBuilder) super.withHttpHeader(header, value);
    }
  }
}
