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
package org.projectnessie.nessie.cli.commands;

import static org.projectnessie.jaxrs.ext.NessieJaxRsExtension.jaxRsExtension;

import java.net.URI;
import java.util.Map;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.nessie.cli.cmdspec.ImmutableConnectCommandSpec;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemorytests.InmemoryBackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
@NessieBackend(InmemoryBackendTestFactory.class)
public abstract class BaseTestCommand {
  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist
  @NessieStoreConfig(name = "namespace-validation", value = "false")
  static Persist persist;

  @RegisterExtension static NessieJaxRsExtension server = jaxRsExtension(() -> persist);

  private URI uri;

  @BeforeEach
  public void setUp(@NessieClientUri URI uri) {
    this.uri = uri;
  }

  protected URI nessieBaseUri() {
    return uri;
  }

  protected NessieCliTester nessieCliTester() throws Exception {
    NessieCliTester tester = unconnectedNessieCliTester();
    tester.execute(
        ImmutableConnectCommandSpec.of(null, nessieBaseUri().toString(), null, Map.of()));
    return tester;
  }

  protected NessieCliTester unconnectedNessieCliTester() throws Exception {
    return new NessieCliTester();
  }
}
