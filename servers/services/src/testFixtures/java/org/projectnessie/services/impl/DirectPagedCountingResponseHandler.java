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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.projectnessie.services.spi.PagedCountingResponseHandler;

final class DirectPagedCountingResponseHandler<E> extends PagedCountingResponseHandler<List<E>, E> {

  final List<E> list = new ArrayList<>();
  final Consumer<String> nextToken;

  public DirectPagedCountingResponseHandler(int max, Consumer<String> nextToken) {
    super(max);
    this.nextToken = nextToken;
  }

  @Override
  public List<E> build() {
    return list;
  }

  @Override
  public void hasMore(String pagingToken) {
    nextToken.accept(pagingToken);
  }

  @Override
  protected boolean doAddEntry(E entry) {
    list.add(entry);
    return true;
  }
}
