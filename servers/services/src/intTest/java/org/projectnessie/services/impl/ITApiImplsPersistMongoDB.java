/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.services.impl;

import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.mongodbtests2.MongoDB2BackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith(PersistExtension.class)
@NessieBackend(MongoDB2BackendTestFactory.class)
@DisabledOnOs(OS.WINDOWS) // testcontainers does not support Windows
public class ITApiImplsPersistMongoDB extends BaseTestApis {}
