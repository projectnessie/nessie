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
package com.dremio.iceberg.client.tag;

import java.util.List;
import java.util.Map;

import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CopyTag implements PendingUpdate<Tag> {

  private final Tag tag;
  private final List<TableIdentifier> updates = Lists.newArrayList();
  private Tag copy;

  public CopyTag(Tag tag) {
    this.tag = tag;
  }

  public CopyTag updateTag(Tag copy) {
    this.copy = copy;
    return this;
  }

  @Override
  public Tag apply() {
    return tag;
  }

  @Override
  public void commit() {
    if (tag == null || !tag.isValid()) {
      return;
    }
    try {
      tag.commit(copy);
    } finally {
      tag.refresh(); // todo if it fails we need to ensure we get old data back
    }
  }
}
