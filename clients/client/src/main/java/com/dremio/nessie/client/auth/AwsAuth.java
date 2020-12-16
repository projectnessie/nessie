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

package com.dremio.nessie.client.auth;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.dremio.nessie.client.http.HttpClient;
import com.dremio.nessie.client.http.RequestFilter;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

public class AwsAuth implements RequestFilter {

  private ObjectMapper objectMapper;
  private final Aws4Signer signer;
  private final AwsCredentialsProvider awsCredentialsProvider;
  private final Region region = Region.US_WEST_2;

  public AwsAuth() {
    this.awsCredentialsProvider = DefaultCredentialsProvider.create();
    this.signer = Aws4Signer.create();
  }

  private SdkHttpFullRequest prepareRequest(String url, HttpClient.Method method, Object entity) {
    try {
      URI uri = new URI(url);
      SdkHttpFullRequest.Builder builder = SdkHttpFullRequest.builder()
                                                             .uri(uri)
                                                             .method(SdkHttpMethod.fromValue(method.name()));

      Arrays.stream(uri.getQuery().split("&")).map(s -> s.split("=")).forEach(s -> builder.putRawQueryParameter(s[0], s[1]));

      if (entity != null) {
        try {
          byte[] bytes = objectMapper.writeValueAsBytes(entity);
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

  @Override
  public void filter(HttpURLConnection con, String url, Map<String, String> headers, HttpClient.Method method, Object body) {
    SdkHttpFullRequest modifiedRequest =
        signer.sign(prepareRequest(url, method, body),
                  Aws4SignerParams.builder()
                                  .signingName("execute-api")
                                  .awsCredentials(awsCredentialsProvider.resolveCredentials())
                                  .signingRegion(region)
                                  .build());
    for (Map.Entry<String, List<String>> entry : modifiedRequest.toBuilder().headers().entrySet()) {
      if (headers.containsKey(entry.getKey())) {
        continue;
      }
      entry.getValue().forEach(a -> headers.put(entry.getKey(), a));
    }
  }

  @Override
  public void init(ObjectMapper mapper) {
    this.objectMapper = mapper;
  }
}
