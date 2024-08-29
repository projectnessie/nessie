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
package org.projectnessie.client;

import static java.util.Collections.singleton;
import static java.util.Locale.ROOT;
import static org.projectnessie.client.NessieConfigConstants.CONF_CONNECT_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_ENABLE_API_COMPATIBILITY_CHECK;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_CLIENT_NAME;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_COMMIT_AUTHORS;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_COMMIT_MESSAGE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_COMMIT_SIGNED_OFF_BY;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_DISABLE_COMPRESSION;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_SNI_HOSTS;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_SNI_MATCHER;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_SSL_CIPHER_SUITES;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_SSL_NO_CERTIFICATE_VERIFICATION;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_SSL_PROTOCOLS;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_TRACING;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;
import static org.projectnessie.client.NessieConfigConstants.CONF_READ_TIMEOUT;
import static org.projectnessie.client.config.NessieClientConfigSources.defaultConfigSources;
import static org.projectnessie.client.config.NessieClientConfigSources.emptyConfigSource;
import static org.projectnessie.client.config.NessieClientConfigSources.systemPropertiesConfigSource;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.auth.NessieAuthenticationProvider;
import org.projectnessie.client.config.NessieClientConfigSource;
import org.projectnessie.client.config.NessieClientConfigSources;

/** {@link NessieApi} builder interface. */
public interface NessieClientBuilder {

  /**
   * The name of the Nessie client implementation, for example {@code HTTP} for the HTTP/REST
   * client.
   */
  String name();

  /** Client names supported by this implementation. */
  default Set<String> names() {
    return singleton(name());
  }

  /**
   * Priority ordinal used to select the most relevant {@link NessieClientBuilder} implementation.
   */
  int priority();

  <I extends NessieClientBuilder> I asInstanceOf(Class<I> builderInterfaceType);

  @CanIgnoreReturnValue
  NessieClientBuilder withApiCompatibilityCheck(boolean enable);

  /**
   * Same semantics as {@link #fromConfig(Function)}, uses the system properties.
   *
   * @return {@code this}
   * @see #fromConfig(Function)
   * @deprecated Use {@link #fromConfig(Function)} with config sources from {@link
   *     NessieClientConfigSources}, preferably {@link
   *     NessieClientConfigSources#defaultConfigSources()}.
   */
  @CanIgnoreReturnValue
  @Deprecated
  NessieClientBuilder fromSystemProperties();

  /**
   * Configure this HttpClientBuilder instance using a configuration object and standard Nessie
   * configuration keys defined by the constants defined in {@link NessieConfigConstants}.
   * Non-{@code null} values returned by the {@code configuration}-function will override previously
   * configured values.
   *
   * <p>Calls {@link #withAuthenticationFromConfig(Function)}.
   *
   * @param configuration The function that returns a configuration value for a configuration key.
   * @return {@code this}
   * @see NessieClientConfigSources
   * @see #withAuthenticationFromConfig(Function)
   */
  @CanIgnoreReturnValue
  NessieClientBuilder fromConfig(Function<String, String> configuration);

  /**
   * Configure only authentication in this HttpClientBuilder instance using a configuration object
   * and standard Nessie configuration keys defined by the constants defined in {@link
   * NessieConfigConstants}.
   *
   * @param configuration The function that returns a configuration value for a configuration key.
   * @return {@code this}
   * @see #fromConfig(Function)
   */
  @CanIgnoreReturnValue
  NessieClientBuilder withAuthenticationFromConfig(Function<String, String> configuration);

  /**
   * Sets the {@link NessieAuthentication} instance to be used.
   *
   * @param authentication authentication for this client
   * @return {@code this}
   */
  @CanIgnoreReturnValue
  NessieClientBuilder withAuthentication(NessieAuthentication authentication);

  @CanIgnoreReturnValue
  NessieClientBuilder withTracing(boolean tracing);

  /**
   * Set the Nessie server URI. A server URI must be configured.
   *
   * @param uri server URI
   * @return {@code this}
   */
  @CanIgnoreReturnValue
  NessieClientBuilder withUri(URI uri);

  /**
   * Convenience method for {@link #withUri(URI)} taking a string.
   *
   * @param uri server URI
   * @return {@code this}
   */
  @CanIgnoreReturnValue
  NessieClientBuilder withUri(String uri);

  /** Sets the read-timeout in milliseconds for remote requests. */
  @CanIgnoreReturnValue
  NessieClientBuilder withReadTimeout(int readTimeoutMillis);

  /** Sets the connect-timeout in milliseconds for remote requests. */
  @CanIgnoreReturnValue
  NessieClientBuilder withConnectionTimeout(int connectionTimeoutMillis);

  /** Disables compression for remote requests. */
  @CanIgnoreReturnValue
  NessieClientBuilder withDisableCompression(boolean disableCompression);

  /**
   * Optional, disables certificate verifications, if set to {@code true}. Can be useful for testing
   * purposes, not recommended for production systems.
   */
  @CanIgnoreReturnValue
  NessieClientBuilder withSSLCertificateVerificationDisabled(
      boolean certificateVerificationDisabled);

  /**
   * Optionally configure a specific {@link SSLContext}, currently only the Java 11+ accepts this
   * option.
   */
  @CanIgnoreReturnValue
  NessieClientBuilder withSSLContext(SSLContext sslContext);

  /** Optionally configure specific {@link SSLParameters}. */
  @CanIgnoreReturnValue
  NessieClientBuilder withSSLParameters(SSLParameters sslParameters);

  /**
   * Optionally configure additional HTTP headers.
   *
   * @param header header name
   * @param value header value
   */
  @CanIgnoreReturnValue
  NessieClientBuilder withHttpHeader(String header, String value);

  /**
   * Registers a future to cancel an ongoing, blocking client setup.
   *
   * <p>When using "blocking" authentication, for example OAuth2 device or code flows, is being
   * used, users may want to cancel an ongoing authentication. An application can register a
   * callback that can be called asynchronously, for example from a SIGINT handler.
   *
   * <p>To implement cancellation:
   *
   * <pre>{@code
   * CompletableFuture<?> cancel = new CompletableFuture<>();
   *
   * registerYourInterruptHandler(cancel::complete);
   *
   * NessieClientBuilder.createClientBuilderFromSystemSettings(...)
   *   .withCancellationFuture(cancel);
   * }</pre>
   */
  @SuppressWarnings("unused")
  @CanIgnoreReturnValue
  NessieClientBuilder withCancellationFuture(CompletionStage<?> cancellationFuture);

  /**
   * Builds a new {@link NessieApi}.
   *
   * @return A new {@link NessieApi}.
   */
  <API extends NessieApi> API build(Class<API> apiContract);

  /**
   * Constructs a client builder instance using config settings from Java system properties, process
   * environment, Nessie client config file {@code ~/.config/nessie/nessie-client.properties},
   * dot-env file {@code ~/.env}.
   */
  static NessieClientBuilder createClientBuilderFromSystemSettings() {
    return createClientBuilderFromSystemSettings(emptyConfigSource());
  }

  static NessieClientBuilder createClientBuilderFromSystemSettings(
      NessieClientConfigSource mainConfigSource) {
    NessieClientConfigSource configSource = mainConfigSource.fallbackTo(defaultConfigSources());
    String clientName = configSource.getValue(CONF_NESSIE_CLIENT_NAME);
    @SuppressWarnings("deprecation")
    String clientBuilderImpl =
        configSource.getValue(NessieConfigConstants.CONF_NESSIE_CLIENT_BUILDER_IMPL);
    return createClientBuilder(clientName, clientBuilderImpl).fromConfig(configSource.asFunction());
  }

  /**
   * Returns the Nessie client builder that matches the requested client name or client builder
   * implementation class.
   *
   * <p>Nessie clients are discovered using Java's {@link ServiceLoader service loader} mechanism.
   *
   * <p>The selection mechanism uses the given {@link NessieClientBuilder#names() Nessie client
   * name} or Nessie client builder implementation class name to select the client builder from the
   * list of available implementations.
   *
   * <p>If neither a name nor an implementation class are specified, aka both parameters are {@code
   * null}, the Nessie client builder with the highest {@link NessieClientBuilder#priority()} will
   * be returned.
   *
   * <p>Either the name or the implementation class should be specified. Specifying both is
   * discouraged.
   *
   * @param clientName the name of the Nessie client, as returned by {@link
   *     NessieClientBuilder#names()}, or {@code null}
   * @param clientBuilderImpl the class that implements the Nessie client builder, or {@code null}
   * @return Nessie client builder for the requested name or implementation class.
   * @throws IllegalArgumentException if no Nessie client matching the requested name and/or
   *     implementation class could be found
   */
  @Nonnull
  @jakarta.annotation.Nonnull
  static NessieClientBuilder createClientBuilder(String clientName, String clientBuilderImpl) {
    ServiceLoader<NessieClientBuilder> implementations =
        ServiceLoader.load(NessieClientBuilder.class);

    String clientNameLower = clientName != null ? clientName.toLowerCase(ROOT) : null;

    List<NessieClientBuilder> builders = new ArrayList<>();
    for (NessieClientBuilder clientBuilder : implementations) {
      builders.add(clientBuilder);
      if (clientNameLower != null
          && clientBuilder.names().stream()
              .map(n -> n.toLowerCase(ROOT))
              .anyMatch(clientNameLower::equals)) {
        if (clientBuilderImpl != null
            && !clientBuilderImpl.isEmpty()
            && !clientBuilder.getClass().getName().equals(clientBuilderImpl)) {
          throw new IllegalArgumentException(
              "Requested client named "
                  + clientName
                  + " does not match requested client builder implementation "
                  + clientBuilderImpl);
        }
        return clientBuilder;
      }
      if (clientBuilder.getClass().getName().equals(clientBuilderImpl)) {
        if (clientNameLower != null && !clientNameLower.isEmpty()) {
          throw new IllegalArgumentException(
              "Requested client builder implementation "
                  + clientBuilderImpl
                  + " does not match requested client named "
                  + clientName);
        }
        return clientBuilder;
      }
    }

    if (clientBuilderImpl != null && clientName == null) {
      // Fallback mechanism for old code that refers to classes that are not mentioned in the file
      // META-INF/services/org.projectnessie.client.NessieClientBuilder, tested via the
      // compatibility tests, so it's possible that old code still uses that mechanism.
      try {
        return (NessieClientBuilder)
            Class.forName(clientBuilderImpl).getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException("Cannot load Nessie client builder implementation class", e);
      }
    }

    if (clientBuilderImpl != null || clientName != null) {
      String msg = "Requested Nessie client";
      msg += " named " + clientName;
      if (clientBuilderImpl != null) {
        msg += " or builder implementation class " + clientBuilderImpl;
      }
      throw new IllegalArgumentException(msg + " not found.");
    }

    builders.sort(Comparator.comparingInt(NessieClientBuilder::priority));
    if (builders.isEmpty()) {
      throw new IllegalStateException(
          "No implementation of " + NessieClientBuilder.class.getName() + " available");
    }
    // Return the client builder with the _highest_ priority value
    return builders.get(builders.size() - 1);
  }

  /** Convenience base class for implementations of {@link NessieClientBuilder}. */
  abstract class AbstractNessieClientBuilder implements NessieClientBuilder {
    @Override
    @Deprecated
    public NessieClientBuilder fromSystemProperties() {
      return fromConfig(systemPropertiesConfigSource().asFunction());
    }

    @Override
    public NessieClientBuilder fromConfig(Function<String, String> configuration) {
      String uri = configuration.apply(CONF_NESSIE_URI);
      if (uri != null) {
        withUri(URI.create(uri));
      }

      withAuthenticationFromConfig(configuration);

      String s = configuration.apply(CONF_NESSIE_TRACING);
      if (s != null) {
        withTracing(Boolean.parseBoolean(s));
      }
      s = configuration.apply(CONF_CONNECT_TIMEOUT);
      if (s != null) {
        withConnectionTimeout(Integer.parseInt(s));
      }
      s = configuration.apply(CONF_READ_TIMEOUT);
      if (s != null) {
        withReadTimeout(Integer.parseInt(s));
      }
      s = configuration.apply(CONF_NESSIE_DISABLE_COMPRESSION);
      if (s != null) {
        withDisableCompression(Boolean.parseBoolean(s));
      }

      s = configuration.apply(CONF_NESSIE_SSL_NO_CERTIFICATE_VERIFICATION);
      if (s != null) {
        withSSLCertificateVerificationDisabled(Boolean.parseBoolean(s));
      }

      SSLParameters sslParameters = new SSLParameters();
      boolean hasSslParameters = false;
      s = configuration.apply(CONF_NESSIE_SSL_CIPHER_SUITES);
      if (s != null) {
        hasSslParameters = true;
        sslParameters.setCipherSuites(
            Arrays.stream(s.split(","))
                .map(String::trim)
                .filter(v -> !v.isEmpty())
                .toArray(String[]::new));
      }
      s = configuration.apply(CONF_NESSIE_SSL_PROTOCOLS);
      if (s != null) {
        hasSslParameters = true;
        sslParameters.setProtocols(
            Arrays.stream(s.split(","))
                .map(String::trim)
                .filter(v -> !v.isEmpty())
                .toArray(String[]::new));
      }
      s = configuration.apply(CONF_NESSIE_SNI_HOSTS);
      if (s != null) {
        hasSslParameters = true;
        sslParameters.setServerNames(
            Arrays.stream(s.split(","))
                .map(String::trim)
                .filter(v -> !v.isEmpty())
                .map(SNIHostName::new)
                .collect(Collectors.toList()));
      }
      s = configuration.apply(CONF_NESSIE_SNI_MATCHER);
      if (s != null) {
        hasSslParameters = true;
        sslParameters.setSNIMatchers(Collections.singletonList(SNIHostName.createSNIMatcher(s)));
      }
      if (hasSslParameters) {
        withSSLParameters(sslParameters);
      }

      s = configuration.apply(CONF_ENABLE_API_COMPATIBILITY_CHECK);
      if (s != null) {
        withApiCompatibilityCheck(Boolean.parseBoolean(s));
      }

      s = configuration.apply(CONF_NESSIE_COMMIT_MESSAGE);
      if (s != null) {
        withHttpHeader("Nessie-Commit-Message", s);
      }
      s = configuration.apply(CONF_NESSIE_COMMIT_AUTHORS);
      if (s != null) {
        withHttpHeader("Nessie-Commit-Authors", s);
      }
      s = configuration.apply(CONF_NESSIE_COMMIT_SIGNED_OFF_BY);
      if (s != null) {
        withHttpHeader("Nessie-Commit-SignedOffBy", s);
      }
      // Note: applying arbitrary properties is not possible here with the single-key approach via
      // `Function<String, String> configuration`.

      return this;
    }

    @Override
    public NessieClientBuilder withAuthenticationFromConfig(
        Function<String, String> configuration) {
      withAuthentication(NessieAuthenticationProvider.fromConfig(configuration));
      return this;
    }

    @Override
    public NessieClientBuilder withUri(String uri) {
      return withUri(URI.create(uri));
    }

    @Override
    public <I extends NessieClientBuilder> I asInstanceOf(Class<I> builderInterfaceType) {
      return builderInterfaceType.cast(this);
    }

    @Override
    public NessieClientBuilder withApiCompatibilityCheck(boolean enable) {
      return this;
    }

    @Override
    public NessieClientBuilder withAuthentication(NessieAuthentication authentication) {
      return this;
    }

    @Override
    public NessieClientBuilder withTracing(boolean tracing) {
      return this;
    }

    @Override
    public NessieClientBuilder withUri(URI uri) {
      return this;
    }

    @Override
    public NessieClientBuilder withReadTimeout(int readTimeoutMillis) {
      return this;
    }

    @Override
    public NessieClientBuilder withConnectionTimeout(int connectionTimeoutMillis) {
      return this;
    }

    @Override
    public NessieClientBuilder withDisableCompression(boolean disableCompression) {
      return this;
    }

    @Override
    public NessieClientBuilder withSSLCertificateVerificationDisabled(
        boolean certificateVerificationDisabled) {
      return this;
    }

    @Override
    public NessieClientBuilder withSSLContext(SSLContext sslContext) {
      return this;
    }

    @Override
    public NessieClientBuilder withSSLParameters(SSLParameters sslParameters) {
      return this;
    }

    public NessieClientBuilder withHttpHeader(String header, String value) {
      return this;
    }

    @Override
    public NessieClientBuilder withCancellationFuture(CompletionStage<?> cancellationFuture) {
      return this;
    }
  }
}
