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
package org.projectnessie.server.catalog.s3;

import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.projectnessie.server.catalog.MinioTestResourceLifecycleManager;

@WithTestResource(
    initArgs = {@ResourceArg(name = "scheme", value = "s3n")},
    value = MinioTestResourceLifecycleManager.class)
@QuarkusIntegrationTest
@TestProfile(ITS3NAssumeRoleIceberg.Profile.class)
public class ITS3NAssumeRoleIceberg extends AbstractAssumeRoleIceberg {
  @Override
  protected String scheme() {
    return "s3n";
  }
}
