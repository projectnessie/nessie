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
package org.projectnessie.quarkus.tests.profiles;

import static io.quarkus.amazon.common.runtime.AwsCredentialsProviderType.STATIC;
import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.DYNAMODB;
import static software.amazon.awssdk.regions.Region.US_WEST_2;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class QuarkusTestProfilePersistDynamo extends BaseConfigProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .putAll(super.getConfigOverrides())
        .put("nessie.version.store.type", DYNAMODB.name())
        .put("quarkus.dynamodb.client.region", US_WEST_2.id())
        .put("quarkus.dynamodb.aws.credentials.type", STATIC.name())
        .put("quarkus.dynamodb.aws.credentials.static-provider.access-key-id", "xxx")
        .put("quarkus.dynamodb.aws.credentials.static-provider.secret-access-key", "xxx")
        .build();
  }

  @Override
  public List<TestResourceEntry> testResources() {
    return Collections.singletonList(
        new TestResourceEntry(DynamoTestResourceLifecycleManager.class));
  }
}
