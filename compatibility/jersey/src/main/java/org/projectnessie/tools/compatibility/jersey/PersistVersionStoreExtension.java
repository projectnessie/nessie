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
package org.projectnessie.tools.compatibility.jersey;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.function.Supplier;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.store.PersistVersionStore;

public class PersistVersionStoreExtension implements Extension {

  private static Supplier<DatabaseAdapter> databaseAdapter;

  public static PersistVersionStoreExtension forDatabaseAdapter(
      Supplier<DatabaseAdapter> databaseAdapter) {
    PersistVersionStoreExtension.databaseAdapter = databaseAdapter;
    return new PersistVersionStoreExtension();
  }

  @SuppressWarnings("unused")
  public void afterBeanDiscovery(@Observes AfterBeanDiscovery abd, BeanManager bm) {

    // Previously, the VersionStore interface had the following signature:
    //   VersionStore<CONTENT, COMMIT_META, CONTENT_TYPE extends Enum<CONTENT_TYPE>>
    // Nessie after 0.41.0 removed the generics from the VersionStore interface, which leads to
    // a _different_ type signature. Have to provide 2 types here, so Weld can properly resolve
    // the VersionStore bean: the "bare" VersionStore type (new one) and the "parameterized" type
    // for Nessie versions up to 0.41.0.

    ParameterizedType pt =
        new ParameterizedType() {
          @Override
          public Type[] getActualTypeArguments() {
            return new Type[] {Content.class, CommitMeta.class, Content.Type.class};
          }

          @Override
          public Type getRawType() {
            return VersionStore.class;
          }

          @Override
          public Type getOwnerType() {
            return null;
          }

          @Override
          public String getTypeName() {
            return VersionStore.class.getName() + "<Content, CommitMeta, Content.Type>";
          }
        };

    // Keep exactly this type and signature to keep construction of PersitVersionStore instance
    // from older Nessie versions working!
    TableCommitMetaStoreWorker storeWorker = new TableCommitMetaStoreWorker();

    abd.addBean()
        .addTypes(pt, VersionStore.class)
        .addQualifier(Default.Literal.INSTANCE)
        .scope(ApplicationScoped.class)
        .produceWith(
            // Keep exactly this type and signature to keep construction of PersistVersionStore
            // instance from older Nessie versions working!
            i -> new PersistVersionStore(databaseAdapter.get(), storeWorker));
  }
}
