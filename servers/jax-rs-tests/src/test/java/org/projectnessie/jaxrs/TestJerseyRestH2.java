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
package org.projectnessie.jaxrs;

import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;
import org.projectnessie.versioned.persist.tx.h2.H2DatabaseAdapterFactory;
import org.projectnessie.versioned.persist.tx.h2.H2TestConnectionProviderSource;

@NessieDbAdapterName(H2DatabaseAdapterFactory.NAME)
@NessieExternalDatabase(H2TestConnectionProviderSource.class)
class TestJerseyRestH2 extends AbstractTestJerseyRest {}
