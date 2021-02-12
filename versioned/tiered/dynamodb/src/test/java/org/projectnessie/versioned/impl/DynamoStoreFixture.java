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
package org.projectnessie.versioned.impl;

import java.net.URI;
import java.net.URISyntaxException;

import org.projectnessie.versioned.dynamodb.DynamoStore;
import org.projectnessie.versioned.dynamodb.DynamoStoreConfig;
import org.projectnessie.versioned.impl.AbstractTieredStoreFixture;

import software.amazon.awssdk.regions.Region;

/**
 * DynamoDB Store fixture.
 *
 * <p>Combine a local dynamodb server with a {@code VersionStore} instance to be used for tests.
 */
public class DynamoStoreFixture extends AbstractTieredStoreFixture<DynamoStore, DynamoStoreConfig> {
  public DynamoStoreFixture() {
    super(makeConfig());
  }

  private static DynamoStoreConfig makeConfig() {
    try {
      return DynamoStoreConfig.builder()
          .endpoint(new URI("http://localhost:8000"))
          .region(Region.US_WEST_2)
          .build();
    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public DynamoStore createStoreImpl() {
    return new DynamoStore(getConfig());
  }

  @Override
  public void close() {
    getStore().deleteTables();
    getStore().close();
  }
}
