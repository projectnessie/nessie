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
package org.projectnessie.tools.compatibility.tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.api.VersionCondition;
import org.projectnessie.tools.compatibility.internal.OlderNessieClientsExtension;

@ExtendWith(OlderNessieClientsExtension.class)
public class ITOlderClients extends AbstractCompatibilityTests {
  @Override
  Version getClientVersion() {
    return version;
  }

  // MergeBehavior is supported by the java client since the release following 0.45.0
  @VersionCondition(minVersion = "0.45.1")
  @Override
  @Test
  public void mergeBehavior() throws BaseNessieClientServerException {
    super.mergeBehavior();
  }
}
