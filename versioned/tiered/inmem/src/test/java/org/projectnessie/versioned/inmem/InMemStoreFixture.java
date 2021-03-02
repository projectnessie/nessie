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
package org.projectnessie.versioned.inmem;

import org.projectnessie.versioned.impl.AbstractTieredStoreFixture;

/**
 * DynamoDB Store fixture.
 *
 * <p>Combine a local dynamodb server with a {@code VersionStore} instance to be used for tests.
 */
public class InMemStoreFixture extends AbstractTieredStoreFixture<InMemStore, String> {
  public InMemStoreFixture() {
    super("");
  }

  @Override
  public InMemStore createStoreImpl() {
    return new InMemStore();
  }

  @Override
  public void close() {
    getStore().close();
  }
}
