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
package org.projectnessie.versioned.storage.cassandra;

import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.projectnessie.versioned.storage.cassandratests.ScyllaDBBackendTestFactory;
import org.projectnessie.versioned.storage.commontests.AbstractVersionStoreTests;
import org.projectnessie.versioned.storage.testextension.NessieBackend;

@DisabledOnOs(
    value = OS.MAC,
    disabledReason =
        "ScyllaDB fails to start, see https://github.com/scylladb/scylladb/issues/10135")
@NessieBackend(ScyllaDBBackendTestFactory.class)
public class ITScyllaDBVersionStore extends AbstractVersionStoreTests {}
