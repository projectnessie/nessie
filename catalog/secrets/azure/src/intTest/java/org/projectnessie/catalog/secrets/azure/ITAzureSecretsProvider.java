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
package org.projectnessie.catalog.secrets.azure;

import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.KeySecret.keySecret;
import static org.projectnessie.catalog.secrets.TokenSecret.tokenSecret;

import com.azure.core.credential.AccessToken;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.security.keyvault.secrets.SecretAsyncClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.TokenSecret;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

@Testcontainers
@ExtendWith(SoftAssertionsExtension.class)
public class ITAzureSecretsProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ITAzureSecretsProvider.class);
  private static final int FLOCI_AZ_PORT = 4577;
  private static final String ACCOUNT = "devstoreaccount1";

  @InjectSoftAssertions SoftAssertions soft;

  @Container
  static GenericContainer<?> flociAz =
      new GenericContainer<>(
              ContainerSpecHelper.builder()
                  .name("floci-az")
                  .containerClass(ITAzureSecretsProvider.class)
                  .build()
                  .dockerImageName(null))
          .withExposedPorts(FLOCI_AZ_PORT)
          .waitingFor(Wait.forHttp("/_floci/health").forPort(FLOCI_AZ_PORT))
          .withLogConsumer(c -> LOGGER.info("[FLOCI-AZ] {}", c.getUtf8StringWithoutLineEnding()));

  @Test
  public void azureSecrets() {
    final SecretAsyncClient client = secretClient();

    String instantStr = "2024-06-05T20:38:16Z";
    Instant instant = Instant.parse(instantStr);

    KeySecret keySecret = keySecret("secret-foo");
    BasicCredentials basicCred = basicCredentials("bar-name", "bar-secret");
    TokenSecret tokenSec = tokenSecret("the-token", instant);

    String key = "key";
    String basic = "basic";
    String tok = "tok";

    client.setSecret(key, "secret-foo").block(Duration.of(1, ChronoUnit.MINUTES));
    client
        .setSecret(basic, "{\"name\": \"bar-name\", \"secret\": \"bar-secret\"}")
        .block(Duration.of(1, ChronoUnit.MINUTES));
    client
        .setSecret(tok, "{\"token\": \"the-token\", \"expiresAt\": \"" + instantStr + "\"}")
        .block(Duration.of(1, ChronoUnit.MINUTES));

    AzureSecretsManager secretsProvider =
        new AzureSecretsManager(client, "", Duration.ofMinutes(1));

    soft.assertThat(secretsProvider.getSecret(key, SecretType.KEY, KeySecret.class))
        .get()
        .asInstanceOf(type(KeySecret.class))
        .extracting(KeySecret::key)
        .isEqualTo(keySecret.key());
    soft.assertThat(secretsProvider.getSecret(basic, SecretType.BASIC, BasicCredentials.class))
        .get()
        .asInstanceOf(type(BasicCredentials.class))
        .extracting(BasicCredentials::name, BasicCredentials::secret)
        .containsExactly(basicCred.name(), basicCred.secret());
    soft.assertThat(secretsProvider.getSecret(tok, SecretType.EXPIRING_TOKEN, TokenSecret.class))
        .get()
        .asInstanceOf(type(TokenSecret.class))
        .extracting(TokenSecret::token, TokenSecret::expiresAt)
        .containsExactly(tokenSec.token(), tokenSec.expiresAt());

    soft.assertThat(secretsProvider.getSecret("not-there", SecretType.KEY, KeySecret.class))
        .isEmpty();
    soft.assertThat(secretsProvider.getSecret("nope", SecretType.BASIC, BasicCredentials.class))
        .isEmpty();
  }

  private static SecretAsyncClient secretClient() {
    String vaultUrl =
        "https://"
            + flociAz.getHost()
            + ':'
            + flociAz.getMappedPort(FLOCI_AZ_PORT)
            + '/'
            + ACCOUNT
            + "-keyvault";
    return new SecretClientBuilder()
        .vaultUrl(vaultUrl)
        .credential(
            request ->
                Mono.just(
                    new AccessToken("fake-token", OffsetDateTime.now(ZoneOffset.UTC).plusHours(1))))
        .addPolicy(new ForceHttpPolicy())
        .disableChallengeResourceVerification()
        .buildAsyncClient();
  }

  static final class ForceHttpPolicy implements HttpPipelinePolicy {
    @Override
    public Mono<HttpResponse> process(
        HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
      URL url = context.getHttpRequest().getUrl();
      if ("https".equals(url.getProtocol())) {
        try {
          context
              .getHttpRequest()
              .setUrl(new URL("http", url.getHost(), url.getPort(), url.getFile()));
        } catch (MalformedURLException e) {
          return Mono.error(e);
        }
      }
      return next.process();
    }
  }
}
