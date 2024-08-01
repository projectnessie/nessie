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
package org.projectnessie.catalog.files.s3;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.projectnessie.catalog.files.s3.S3Utils.iamEscapeString;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.checkerframework.checker.index.qual.NonNegative;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/** Maintains a pool of STS clients and manages refreshing session credentials on demand. */
public class S3SessionsManager {
  public static final String CLIENTS_CACHE_NAME = "sts-clients";
  public static final String SESSIONS_CACHE_NAME = "sts-sessions";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Cache<StsClientKey, StsClient> clients;
  private final LoadingCache<SessionKey, Credentials> sessions;
  private final Function<StsClientKey, StsClient> clientBuilder;
  private final Duration expiryReduction;
  private final SessionCredentialsFetcher sessionCredentialsFetcher;

  public S3SessionsManager(
      S3Options options, SdkHttpClient sdkHttpClient, MeterRegistry meterRegistry) {
    this(
        options,
        System::currentTimeMillis,
        sdkHttpClient,
        null,
        Optional.ofNullable(meterRegistry),
        null);
  }

  @VisibleForTesting
  S3SessionsManager(
      S3Options options,
      LongSupplier systemTimeMillis,
      SdkHttpClient sdkHttpClient,
      Function<StsClientKey, StsClient> clientBuilder,
      Optional<MeterRegistry> meterRegistry,
      SessionCredentialsFetcher sessionCredentialsFetcher) {
    this.clientBuilder =
        clientBuilder != null ? clientBuilder : (parameters) -> client(parameters, sdkHttpClient);
    this.expiryReduction = options.effectiveSessionCredentialRefreshGracePeriod();
    this.sessionCredentialsFetcher =
        sessionCredentialsFetcher != null
            ? sessionCredentialsFetcher
            : this::executeAssumeRoleRequest;

    // Cache clients without expiration time keyed by the parameters that cannot be adjusted
    // per-request. Note: Credentials are set individually in each STS request.
    this.clients =
        Caffeine.newBuilder()
            .maximumSize(options.effectiveStsClientsCacheMaxEntries())
            .recordStats(
                () ->
                    statsCounter(
                        meterRegistry,
                        CLIENTS_CACHE_NAME,
                        options.effectiveStsClientsCacheMaxEntries()))
            .build();

    this.sessions =
        Caffeine.newBuilder()
            .ticker(() -> TimeUnit.MILLISECONDS.toNanos(systemTimeMillis.getAsLong()))
            .maximumSize(options.effectiveSessionCredentialCacheMaxEntries())
            .recordStats(
                () ->
                    statsCounter(
                        meterRegistry,
                        SESSIONS_CACHE_NAME,
                        options.effectiveSessionCredentialCacheMaxEntries()))
            .expireAfter(
                new Expiry<SessionKey, Credentials>() {
                  @Override
                  public long expireAfterCreate(
                      SessionKey key, Credentials value, long currentTimeNanos) {
                    return lifetimeNanos(value, currentTimeNanos);
                  }

                  @Override
                  public long expireAfterUpdate(
                      SessionKey key,
                      Credentials value,
                      long currentTime,
                      @NonNegative long currentDuration) {
                    return currentDuration;
                  }

                  @Override
                  public long expireAfterRead(
                      SessionKey key,
                      Credentials value,
                      long currentTime,
                      @NonNegative long currentDuration) {
                    return currentDuration;
                  }
                })
            .build(this::loadSession);
  }

  private StatsCounter statsCounter(
      Optional<MeterRegistry> meterRegistry, String name, int maxSize) {
    if (meterRegistry.isPresent()) {
      meterRegistry
          .get()
          .gauge("max_entries", singletonList(Tag.of("cache", name)), "", x -> maxSize);

      return new CaffeineStatsCounter(meterRegistry.get(), name);
    }

    return StatsCounter.disabledStatsCounter();
  }

  private long lifetimeNanos(Credentials credentials, long currentTimeNanos) {
    Instant expiration = credentials.expiration();
    long currentTimeMillis = TimeUnit.NANOSECONDS.toMillis(currentTimeNanos);
    currentTimeMillis += expiryReduction.toMillis();
    Instant effectiveNow = Instant.ofEpochMilli(currentTimeMillis);

    long lifetimeMillis;
    if (expiration.isBefore(effectiveNow)) {
      lifetimeMillis = 0;
    } else {
      lifetimeMillis = effectiveNow.until(expiration, ChronoUnit.MILLIS);
    }
    return TimeUnit.MILLISECONDS.toNanos(lifetimeMillis);
  }

  private Credentials loadSession(SessionKey sessionKey) {
    // Cached sessions use the default duration.
    return fetchCredentials(sessionKey, Optional.empty());
  }

  private Credentials fetchCredentials(SessionKey sessionKey, Optional<Duration> sessionDuration) {
    // Note: StsClients may be shared across repositories.
    String region =
        sessionKey
            .bucketOptions()
            .region()
            .orElseThrow(() -> new IllegalArgumentException("Missing S3 region"));
    StsClientKey clientKey =
        ImmutableStsClientKey.of(sessionKey.bucketOptions().stsEndpoint(), region);
    StsClient client = clients.get(clientKey, clientBuilder);

    return sessionCredentialsFetcher.fetchCredentials(client, sessionKey, sessionDuration);
  }

  private Credentials executeAssumeRoleRequest(
      StsClient client, SessionKey sessionKey, Optional<Duration> sessionDuration) {
    AssumeRoleRequest.Builder request = AssumeRoleRequest.builder();
    S3BucketOptions bucketOptions = sessionKey.bucketOptions();
    request.roleSessionName(
        bucketOptions.roleSessionName().orElse(S3BucketOptions.DEFAULT_SESSION_NAME));
    bucketOptions.assumeRole().ifPresent(request::roleArn);
    bucketOptions.externalId().ifPresent(request::externalId);
    bucketOptions.sessionIamPolicy().ifPresent(request::policy);
    sessionDuration.ifPresent(
        duration -> {
          long seconds = duration.toSeconds();
          checkArgument(
              seconds < Integer.MAX_VALUE, "Requested session duration is too long: " + duration);
          request.durationSeconds((int) seconds);
        });

    request.overrideConfiguration(
        builder -> {
          S3ServerAuthenticationMode authMode = bucketOptions.effectiveServerAuthenticationMode();
          builder.credentialsProvider(authMode.newCredentialsProvider(bucketOptions));
        });

    AssumeRoleResponse response = client.assumeRole(request.build());
    return response.credentials();
  }

  public static String locationDependentPolicy(
      StorageLocations locations, Optional<List<String>> clientIamStatements) {

    // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/security_iam_service-with-iam.html
    // See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies.html
    // See https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html

    // Add necessary "Allow" statements for the given location. Use Jackson's mechanics here to
    // benefit from its proper JSON value escaping.

    ObjectNode policy = JsonNodeFactory.instance.objectNode();
    policy.put("Version", "2012-10-17");

    ArrayNode statements = policy.withArray("Statement");

    // "Allow" for the special 's3:listBucket' case, must be applied on the bucket, not the path.
    for (Iterator<StorageUri> locationIter =
            Stream.concat(
                    locations.writeableLocations().stream(), locations.readonlyLocations().stream())
                .iterator();
        locationIter.hasNext(); ) {
      StorageUri location = locationIter.next();
      String bucket = iamEscapeString(location.requiredAuthority());
      String path = iamEscapeString(location.pathWithoutLeadingTrailingSlash());

      ObjectNode statement = statements.addObject();
      statement.put("Effect", "Allow");
      statement.put("Action", "s3:ListBucket");
      statement.put("Resource", format("arn:aws:s3:::%s", bucket));
      ObjectNode condition = statement.withObject("Condition");
      ObjectNode stringLike = condition.withObject("StringLike");
      ArrayNode s3Prefix = stringLike.withArray("s3:prefix");
      s3Prefix.add(path);
      s3Prefix.add(path + "/*");
      // For write.object-storage.enabled=true
      s3Prefix.add("*/" + path);
      s3Prefix.add("*/" + path + "/*");
    }

    // "Allow Write" for all remaining S3 actions on the bucket+path.
    List<StorageUri> writeable = locations.writeableLocations();
    if (!writeable.isEmpty()) {
      ObjectNode statement = statements.addObject();
      statement.put("Effect", "Allow");
      ArrayNode actions = statement.putArray("Action");
      actions.add("s3:GetObject");
      actions.add("s3:GetObjectVersion");
      actions.add("s3:PutObject");
      actions.add("s3:DeleteObject");
      ArrayNode resources = statement.withArray("Resource");
      for (StorageUri location : writeable) {
        String bucket = iamEscapeString(location.requiredAuthority());
        String path = iamEscapeString(location.pathWithoutLeadingTrailingSlash());
        resources.add(format("arn:aws:s3:::%s/%s/*", bucket, path));
        // For write.object-storage.enabled=true
        resources.add(format("arn:aws:s3:::%s/*/%s/*", bucket, path));
      }
    }

    // "Allow read" for all remaining S3 actions on the bucket+path.
    List<StorageUri> readonly = locations.readonlyLocations();
    if (!readonly.isEmpty()) {
      ObjectNode statement = statements.addObject();
      statement.put("Effect", "Allow");
      ArrayNode actions = statement.putArray("Action");
      actions.add("s3:GetObject");
      actions.add("s3:GetObjectVersion");
      ArrayNode resources = statement.withArray("Resource");
      for (StorageUri location : readonly) {
        String bucket = iamEscapeString(location.requiredAuthority());
        String path = iamEscapeString(location.pathWithoutLeadingTrailingSlash());
        resources.add(format("arn:aws:s3:::%s%s/*", bucket, path));
        // For write.object-storage.enabled=true
        resources.add(format("arn:aws:s3:::%s/*/%s/*", bucket, path));
      }
    }

    // Add custom statements
    clientIamStatements.ifPresent(
        stmts -> stmts.stream().map(ParsedIamStatements.STATEMENTS::get).forEach(statements::add));

    return policy.toString();
  }

  static class ParsedIamStatements {
    static final LoadingCache<String, ObjectNode> STATEMENTS =
        Caffeine.newBuilder()
            .maximumSize(2000)
            .expireAfterAccess(Duration.of(1, ChronoUnit.HOURS))
            .build(ParsedIamStatements::parseStatement);

    private static ObjectNode parseStatement(String stmt) {
      try (MappingIterator<Object> values = MAPPER.readerFor(ObjectNode.class).readValues(stmt)) {
        ObjectNode node = null;
        if (values.hasNext()) {
          Object value = values.nextValue();
          if (value instanceof ObjectNode) {
            node = (ObjectNode) value;
          } else {
            throw new IOException("Invalid statement");
          }
        }
        if (values.hasNext()) {
          throw new IOException("Invalid statement");
        }
        return node;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  Credentials sessionCredentialsForServer(String repositoryId, S3BucketOptions options) {
    SessionKey sessionKey = buildSessionKey(repositoryId, options);
    return sessions.get(sessionKey);
  }

  Credentials sessionCredentialsForClient(String repositoryId, S3BucketOptions options) {
    SessionKey sessionKey = buildSessionKey(repositoryId, options);
    // In this case cache only the client, but not the credentials. Session cache hits are
    // unlikely in the latter case as we'd have to include the exact expiry instant (not session
    // duration) in the SessionKey, otherwise we risk reusing credentials that are about to expire
    // for a fresh (long-running) session.
    return fetchCredentials(sessionKey, options.clientSessionDuration());
  }

  private static SessionKey buildSessionKey(String repositoryId, S3BucketOptions options) {
    // Client parameters are part of the credential's key because clients in different regions may
    // issue different credentials.
    return ImmutableSessionKey.builder().repositoryId(repositoryId).bucketOptions(options).build();
  }

  private static StsClient client(StsClientKey parameters, SdkHttpClient sdkClient) {
    StsClientBuilder builder = StsClient.builder();
    builder.httpClient(sdkClient);
    // TODO the URI path of the endpoint will get LOST if configured via
    //  StsClientBuilder.endpointOverride(), but not when provided via an endpoint.provider.
    // parameters.endpoint().ifPresent(builder::endpointOverride);
    if (parameters.endpoint().isPresent()) {
      CompletableFuture<Endpoint> endpointFuture =
          completedFuture(Endpoint.builder().url(parameters.endpoint().get()).build());
      builder.endpointProvider(params -> endpointFuture);
    }
    builder.region(Region.of(parameters.region()));
    return builder.build();
  }

  @NessieImmutable
  interface SessionKey {
    String repositoryId();

    S3BucketOptions bucketOptions();
  }

  @NessieImmutable
  interface StsClientKey {
    Optional<URI> endpoint();

    String region();
  }

  @FunctionalInterface
  interface SessionCredentialsFetcher {
    Credentials fetchCredentials(
        StsClient client, SessionKey sessionKey, Optional<Duration> sessionDuration);
  }
}
