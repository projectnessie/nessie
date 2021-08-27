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
package org.projectnessie.server;

import io.quarkus.test.junit.NativeImageTest;
import io.quarkus.test.junit.TestProfile;

@NativeImageTest
@TestProfile(value = ITNativeOpenIdAuthentication.Profile.class)
public class ITNativeOpenIdAuthentication extends TestOpenIdAuthentication {
  public static class Profile extends TestOpenIdAuthentication.Profile {
    @Override
    public String getConfigProfile() {
      return "prod";
    }
  }
}
