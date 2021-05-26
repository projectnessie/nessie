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
package org.projectnessie.hms;

import java.util.Optional;
import java.util.function.Supplier;
import org.apache.hadoop.hive.metastore.RawStore;

class RoutingTransactionHandler implements TransactionHandler {

  Supplier<NessieStore> nessie;
  Supplier<Optional<RawStore>> delegate;

  RoutingTransactionHandler(Supplier<NessieStore> nessie, Supplier<Optional<RawStore>> delegate) {
    super();
    this.nessie = nessie;
    this.delegate = delegate;
  }

  public void routedDelegate() {}

  public void routedNessie() {}

  private boolean hasDelegate() {
    return delegate.get().isPresent();
  }

  @Override
  public boolean isActiveTransaction() {
    boolean isActive = nessie.get().isActiveTransaction();

    if (!hasDelegate()) {
      return isActive;
    }

    return delegate.get().get().isActiveTransaction() && isActive;
  }

  @Override
  public void rollbackTransaction() {}

  @Override
  public boolean openTransaction() {
    boolean ret = nessie.get().openTransaction();
    delegate.get().ifPresent(r -> r.openTransaction());
    return ret;
  }

  @Override
  public boolean commitTransaction() {
    boolean ret = nessie.get().commitTransaction();
    delegate.get().ifPresent(r -> r.commitTransaction());
    return ret;
  }

  @Override
  public void shutdown() {
    nessie.get().shutdown();
    delegate.get().ifPresent(r -> r.shutdown());
  }
}
