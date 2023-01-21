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
package org.projectnessie.services.spi;

public abstract class PagedCountingResponseHandler<R, E> implements PagedResponseHandler<R, E> {
  private final int max;
  private int cnt;

  public PagedCountingResponseHandler(Integer maxRecords) {
    this.max = maxRecords != null ? maxRecords : Integer.MAX_VALUE;
  }

  public PagedCountingResponseHandler(Integer maxRecords, int strictMax) {
    this.max = Math.min(maxRecords != null ? Math.max(maxRecords, 0) : strictMax, strictMax);
  }

  protected abstract boolean doAddEntry(E entry);

  @Override
  public final boolean addEntry(E entry) {
    if (max > 0 && cnt >= max) {
      return false;
    }
    if (doAddEntry(entry)) {
      cnt++;
      return true;
    }
    return false;
  }
}
