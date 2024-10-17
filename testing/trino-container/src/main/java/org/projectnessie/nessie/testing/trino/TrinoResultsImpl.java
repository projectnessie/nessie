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
package org.projectnessie.nessie.testing.trino;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.AbstractIterator;
import io.trino.client.Column;
import io.trino.client.QueryError;
import io.trino.client.QueryStatusInfo;
import io.trino.client.StatementClient;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

final class TrinoResultsImpl extends AbstractIterator<List<Object>> implements TrinoResults {
  private final StatementClient client;
  private final AtomicBoolean closed = new AtomicBoolean();

  private Iterator<List<Object>> currentPage;

  TrinoResultsImpl(StatementClient client) {
    this.client = client;
  }

  @Override
  protected List<Object> computeNext() {
    if (closed.get()) {
      return endOfData();
    }

    while (true) {
      if (currentPage != null) {
        if (currentPage.hasNext()) {
          return currentPage.next();
        }
        currentPage = null;
      }

      QueryStatusInfo results;
      QueryError error;
      while (client.isRunning()) {
        results = client.currentStatusInfo();
        error = results.getError();
        if (error != null) {
          throw error.getFailureInfo().toException();
        }
        Iterable<List<Object>> data = client.currentRows();

        client.advance();

        if (data != null) {
          currentPage = data.iterator();
          break;
        } else {
          try {
            Thread.sleep(100L);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      }

      if (currentPage != null) {
        continue;
      }

      checkState(client.isFinished(), "Got no data, but client has finished");
      results = client.finalStatusInfo();
      error = results.getError();
      if (error != null) {
        throw error.getFailureInfo().toException();
      }

      close();

      return endOfData();
    }
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      client.close();
    }
  }

  @Override
  public StatementClient client() {
    return client;
  }

  @Override
  public List<Column> columns() {
    return client.currentStatusInfo().getColumns();
  }

  @Override
  public List<List<Object>> allRows() {
    List<List<Object>> result = new ArrayList<>();
    this.forEachRemaining(result::add);
    return result;
  }
}
