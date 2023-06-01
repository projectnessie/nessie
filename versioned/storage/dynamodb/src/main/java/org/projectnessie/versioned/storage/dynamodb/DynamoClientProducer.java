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
package org.projectnessie.versioned.storage.dynamodb;

import java.net.URI;
import org.immutables.value.Value;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

@Value.Immutable
public abstract class DynamoClientProducer {

  public static ImmutableDynamoClientProducer.Builder builder() {
    return ImmutableDynamoClientProducer.builder();
  }

  abstract String endpointURI();

  abstract String region();

  abstract AwsCredentialsProvider credentialsProvider();

  public DynamoDbClient createClient() {
    DynamoDbClientBuilder clientBuilder =
        DynamoDbClient.builder()
            .httpClientBuilder(ApacheHttpClient.builder())
            .region(Region.of(region()));

    AwsCredentialsProvider credentialsProvider = credentialsProvider();
    if (credentialsProvider != null) {
      clientBuilder = clientBuilder.credentialsProvider(credentialsProvider);
    }
    String endpointURI = endpointURI();
    if (endpointURI != null) {
      clientBuilder = clientBuilder.endpointOverride(URI.create(endpointURI));
    }

    return clientBuilder.build();
  }
}
