/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.cli.commands;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TOKEN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TYPE;
import static org.projectnessie.client.http.impl.HttpUtils.checkArgument;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_BLUE;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_FAINT;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_YELLOW;

import jakarta.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.aws.s3.signer.S3V4RestSignerClient;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.config.NessieClientConfigSources;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.ConnectCommandSpec;
import org.projectnessie.nessie.cli.grammar.Node;
import org.projectnessie.nessie.cli.grammar.Token;

public class ConnectCommand extends NessieCommand<ConnectCommandSpec> {

  static final Pattern NESSIE_URI_PATTERN = Pattern.compile("^(http.*/)api/v[1-5]/?" + "$");

  public ConnectCommand() {}

  @Override
  public void execute(@Nonnull BaseNessieCli cli, ConnectCommandSpec spec) throws Exception {

    PrintWriter writer = cli.writer();

    String uri = spec.getUri();

    RESTCatalog icebergClient = null;
    NessieApiV2 api = null;
    boolean failure = true;
    String initialReference = spec.getInitialReference();
    Map<String, String> connectOptions = spec.getParameters();
    Map<String, String> icebergProperties = new HashMap<>(connectOptions);
    Map<String, String> nessieOptions = new HashMap<>(connectOptions);

    try {
      writer.println(
          new AttributedString(format("Connecting to %s ...", uri), STYLE_FAINT)
              .toAnsi(cli.terminal()));
      writer.flush();

      String nessieUri = uri;
      String icebergUri = uri;

      // Check if 'uri' represents a Nessie API base URI, if so, derive the Iceberg REST base URI.
      Matcher uriMatcher = NESSIE_URI_PATTERN.matcher(uri);
      boolean isNessieURI = uriMatcher.matches();
      if (isNessieURI) {
        icebergUri = uriMatcher.group(1) + "iceberg/";
      }

      // Derive Iceberg options from Nessie connect options and vice versa.
      String icebergToken = connectOptions.get("token");
      String nessieToken = connectOptions.get(CONF_NESSIE_AUTH_TOKEN);
      checkArgument(
          icebergToken == null || nessieToken == null || icebergToken.equals(nessieToken),
          "Supplied both 'token' and '%s' options, but with different values. Remove one of those options or use the same value.",
          CONF_NESSIE_AUTH_TOKEN);
      String bearerToken = icebergToken != null ? icebergToken : nessieToken;
      String nessieAuthType = connectOptions.get(CONF_NESSIE_AUTH_TYPE);
      if (bearerToken != null) {
        if (nessieAuthType == null) {
          nessieOptions.put(CONF_NESSIE_AUTH_TYPE, "BEARER");
        } else {
          checkArgument(
              "BEARER".equals(nessieAuthType),
              "Must use Nessie auth type %s = BEARER when providing a bearer token",
              CONF_NESSIE_AUTH_TYPE);
        }

        // "Propagate" the bearer token to both Nessie + Iceberg clients.
        icebergProperties.put("token", bearerToken);
        nessieOptions.put(CONF_NESSIE_AUTH_TOKEN, bearerToken);
      }

      // Ask the user to enter the token, if BEARER auth is configured.
      if ("BEARER".equals(nessieAuthType) && bearerToken == null) {
        writer.println(
            new AttributedString(
                    "\nUsing BEARER authentication, enter token (not echoed) + press ENTER:",
                    STYLE_BLUE.bold())
                .toAnsi(cli.terminal()));
        writer.flush();
        bearerToken = new String(System.console().readPassword()).trim();

        String warehouse = connectOptions.get("warehouse");
        if (warehouse != null) {
          icebergProperties.put("warehouse", warehouse);
        }

        nessieOptions.put(CONF_NESSIE_AUTH_TOKEN, bearerToken);
        icebergProperties.put("token", bearerToken);
      }

      try {
        icebergProperties.put("uri", icebergUri);

        // Use the "initial reference" clause from the CONNECT TO statement as the 'prefix'.
        if (initialReference != null) {
          icebergProperties.put("prefix", URLEncoder.encode(initialReference, UTF_8));
        }

        // Collect stdout+stderr to prevent the annoying exception printed by
        // org.apache.iceberg.rest.ErrorHandlers.DefaultErrorHandler when the remote endpoint does
        // not exist -  for example when connecting to an older Nessie instance w/o Iceberg REST.
        ByteArrayOutputStream errorCollector = new ByteArrayOutputStream();
        try {
          PrintStream out = System.out;
          PrintStream err = System.err;
          try (PrintStream ps = new PrintStream(errorCollector)) {
            System.setOut(ps);
            System.setErr(ps);
            // Having the 'new RESTCatalog()' here prevents the 'WARNING:
            // PropertyNamingStrategy.KebabCaseStrategy ...' message
            icebergClient = new RESTCatalog();
            icebergClient.initialize("iceberg", icebergProperties);

            cleanupS3V4RestSignerClient(cli);
          } finally {
            System.setOut(out);
            System.setErr(err);
          }
        } catch (RESTException e) {
          throw e;
        } catch (Exception e) {
          writer.print(errorCollector);
          writer.flush();
          throw e;
        }

        Map<String, String> properties = icebergClient.properties();
        if (Boolean.parseBoolean(properties.get("nessie.is-nessie-catalog"))) {
          nessieUri = properties.get("nessie.core-base-uri") + "v2/";
          if (initialReference == null) {
            initialReference = properties.get("nessie.default-branch.name");
          }
        }
        // Note: at one point we might extend the Nessie-CLI to work against any Iceberg-REST
        // service, but not yet. Contributions are welcome.

        writer.printf("Successfully connected to Iceberg REST at %s%n", icebergUri);
        writer.println(
            new AttributedString(
                    format("Connecting to Nessie REST at %s ...", nessieUri), STYLE_FAINT)
                .toAnsi(cli.terminal()));
        writer.flush();
      } catch (RESTException e) {
        writer.println(
            new AttributedString(
                    format("No Iceberg REST endpoint at %s ...", icebergUri), STYLE_YELLOW)
                .toAnsi(cli.terminal()));
        writer.flush();

        if (icebergClient != null) {
          icebergClient.close();
          icebergClient = null;
        }
      }

      NessieConfiguration config;

      CompletableFuture<?> cancel = new CompletableFuture<>();
      Terminal terminal = cli.terminal();
      Terminal.SignalHandler sigIntHandler =
          terminal.handle(Terminal.Signal.INT, sig -> cancel.completeAsync(() -> null));
      try {
        api =
            NessieClientBuilder.createClientBuilderFromSystemSettings(
                    NessieClientConfigSources.mapConfigSource(nessieOptions))
                .withUri(nessieUri)
                .withCancellationFuture(cancel)
                .build(NessieApiV2.class);

        config = api.getConfig();
      } catch (Exception e) {
        if (api != null) {
          api.close();
        }

        if (hasCauseMatching(
            e,
            t ->
                t instanceof CancellationException
                    || t instanceof InterruptedException
                    || t instanceof TimeoutException)) {
          writer.println(
              new AttributedString("Connection request aborted or timed out.", STYLE_YELLOW)
                  .toAnsi(cli.terminal()));
          writer.println();
          writer.flush();

          return;
        }
        throw e;
      } finally {
        if (sigIntHandler != null) {
          terminal.handle(Terminal.Signal.INT, sigIntHandler);
        }
      }

      writer.printf(
          "Successfully connected to Nessie REST at %s - Nessie API version %d, spec version %s%n",
          nessieUri, config.getActualApiVersion(), config.getSpecVersion());

      cli.connected(api, icebergClient);
      failure = false;

      Reference ref;
      if (initialReference != null) {
        ref = api.getReference().refName(initialReference).get();
      } else {
        ref = api.getDefaultBranch();
      }

      cli.setCurrentReference(ref);
    } finally {
      if (failure) {
        try {
          if (api != null) {
            api.close();
          }
        } catch (Exception e) {
          // ignore
        }
        try {
          if (icebergClient != null) {
            icebergClient.close();
          }
        } catch (Exception e) {
          // ignore
        }
      }
    }
  }

  static boolean hasCauseMatching(Throwable t, Predicate<Throwable> test) {
    for (; t != null; t = t.getCause()) {
      if (test.test(t)) {
        return true;
      }
    }
    return false;
  }

  public String name() {
    return Token.TokenType.CONNECT.name();
  }

  public String description() {
    return "Connect to a Nessie repository.";
  }

  @Override
  public List<List<Node.NodeType>> matchesNodeTypes() {
    return List.of(List.of(Token.TokenType.CONNECT));
  }

  /**
   * Clean up the {@link S3V4RestSignerClient} static fields which keep auth information, which
   * breaks running with different auth setups. This is caused by <a
   * href="https://github.com/apache/iceberg/pull/13215">PR 13215</a> since Iceberg 1.10.
   */
  static void cleanupS3V4RestSignerClient(@Nonnull BaseNessieCli cli) {
    try {
      Class<S3V4RestSignerClient> c = S3V4RestSignerClient.class;

      Field f = c.getDeclaredField("authManager");
      if (f.getModifiers() == java.lang.reflect.Modifier.STATIC) {
        f.setAccessible(true);
        AuthManager authManager = (AuthManager) f.get(null);
        if (authManager != null) {
          f.set(null, null);
          authManager.close();
        }
      }

      f = c.getDeclaredField("httpClient");
      if (f.getModifiers() == java.lang.reflect.Modifier.STATIC) {
        f.setAccessible(true);
        RESTClient httpClient = (RESTClient) f.get(null);
        if (httpClient != null) {
          f.set(null, null);
          httpClient.close();
        }
      }

      f = c.getDeclaredField("SIGNED_COMPONENT_CACHE");
      f.setAccessible(true);
      Object cache = f.get(null);
      if (cache != null) {
        Method invalidateAllMethod = f.getType().getDeclaredMethod("invalidateAll");
        invalidateAllMethod.invoke(cache);
      }
    } catch (Exception e) {
      PrintWriter writer = cli.writer();
      writer.printf("Failed to cleaned up " + S3V4RestSignerClient.class.getName());
      e.printStackTrace(writer);
    }
  }
}
