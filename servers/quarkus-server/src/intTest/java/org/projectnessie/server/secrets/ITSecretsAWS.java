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
package org.projectnessie.server.secrets;

import static java.util.Locale.ROOT;
import static org.projectnessie.quarkus.config.QuarkusSecretsConfig.SecretsSupplierType.AMAZON;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest // because the tests need @Inject'd fields
@WithTestResource(LocalstackTestResourceLifecycleManager.class)
public class ITSecretsAWS extends AbstractSecretsSuppliers {
  @Override
  protected String providerName() {
    return AMAZON.name().toLowerCase(ROOT);
  }
}
