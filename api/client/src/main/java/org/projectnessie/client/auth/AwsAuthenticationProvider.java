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
package org.projectnessie.client.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.RequestContext;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.regions.Region;

/**
 * AWS authentication provider.
 *
 * <p>Takes parameters {@link
 * org.projectnessie.client.NessieConfigConstants#CONF_NESSIE_AWS_REGION}.
 */
public class AwsAuthenticationProvider implements NessieAuthenticationProvider {

  public static final String AUTH_TYPE_VALUE = "AWS";

  public static HttpAuthentication create(Region region) {
    return create(region, null);
  }

  public static HttpAuthentication create(Region region, String profile) {
    return new AwsAuthentication(region, profile);
  }

  @Override
  public String getAuthTypeValue() {
    return AUTH_TYPE_VALUE;
  }

  @Override
  public HttpAuthentication build(Function<String, String> parameters) {
    String regionName = parameters.apply(NessieConfigConstants.CONF_NESSIE_AWS_REGION);
    if (regionName == null) {
      regionName = Region.US_WEST_2.toString();
    }

    Region region = awsRegion(regionName);

    String profile = parameters.apply(NessieConfigConstants.CONF_NESSIE_AWS_PROFILE);

    return create(region, profile);
  }

  private static Region awsRegion(String regionName) {
    return Region.regions().stream()
        .filter(r -> r.toString().equalsIgnoreCase(regionName))
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException(String.format("Unknown region '%s'.", regionName)));
  }

  private static class AwsAuthentication implements HttpAuthentication {
    private final Region region;
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final ObjectMapper objectMapper;

    private AwsAuthentication(Region region, String profile) {
      this.region = region;

      DefaultCredentialsProvider.Builder provider = DefaultCredentialsProvider.builder();
      if (profile != null) {
        provider.profileName(profile);
      }

      this.awsCredentialsProvider = provider.build();
      this.objectMapper =
          new ObjectMapper()
              .disable(SerializationFeature.INDENT_OUTPUT)
              .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    @Override
    public void applyToHttpClient(HttpClient.Builder client) {
      client.addRequestFilter(this::applyToHttpRequest);
    }

    @Override
    public void applyToHttpRequest(RequestContext context) {
      SdkHttpFullRequest request =
          prepareRequest(context.getUri(), context.getMethod(), context.getBody());
      AwsCredentials identity = awsCredentialsProvider.resolveCredentials();
      String signingName = "execute-api";
      SdkHttpRequest modReq =
          AwsV4HttpSigner.create()
              .sign(
                  b -> {
                    b.request(request)
                        .identity(identity)
                        .putProperty(AwsV4HttpSigner.REGION_NAME, region.id())
                        .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, signingName);
                    request.contentStreamProvider().ifPresent(b::payload);
                  })
              .request();
      for (Map.Entry<String, List<String>> entry : modReq.headers().entrySet()) {
        if (context.containsHeader(entry.getKey())) {
          continue;
        }
        entry.getValue().forEach(a -> context.putHeader(entry.getKey(), a));
      }
    }

    private SdkHttpFullRequest prepareRequest(
        URI uri, HttpClient.Method method, Optional<Object> entity) {
      try {
        SdkHttpFullRequest.Builder builder =
            SdkHttpFullRequest.builder().uri(uri).method(SdkHttpMethod.fromValue(method.name()));

        String query = uri.getQuery();
        if (query != null) {
          Arrays.stream(query.split("&"))
              .map(s -> s.split("="))
              .forEach(s -> builder.putRawQueryParameter(s[0], s[1]));
        }

        if (entity.isPresent()) {
          try {
            byte[] bytes = objectMapper.writeValueAsBytes(entity.get());
            builder.contentStreamProvider(() -> new ByteArrayInputStream(bytes));
          } catch (Throwable t) {
            throw new RuntimeException(t);
          }
        }
        return builder.build();
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }
}
