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
package org.projectnessie.versioned;

import java.util.function.Supplier;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.util.TypeLiteral;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.store.PersistVersionStore;

/** This class needs to be in the same package as {@link VersionStore}. */
public class PersistVersionStoreExtension implements Extension {

  private static Supplier<DatabaseAdapter> databaseAdapter;

  public static PersistVersionStoreExtension forDatabaseAdapter(
      Supplier<DatabaseAdapter> databaseAdapter) {
    PersistVersionStoreExtension.databaseAdapter = databaseAdapter;
    return new PersistVersionStoreExtension();
  }

  @SuppressWarnings("unused")
  public void afterBeanDiscovery(@Observes AfterBeanDiscovery abd, BeanManager bm) {
    TableCommitMetaStoreWorker storeWorker = new TableCommitMetaStoreWorker();

    abd.addBean()
        .addType(new TypeLiteral<VersionStore<Contents, CommitMeta, Type>>() {})
        .addQualifier(Default.Literal.INSTANCE)
        .scope(ApplicationScoped.class)
        .produceWith(i -> new PersistVersionStore<>(databaseAdapter.get(), storeWorker));
  }
}
