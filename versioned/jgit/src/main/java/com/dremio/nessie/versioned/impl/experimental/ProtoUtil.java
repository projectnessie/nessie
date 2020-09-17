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

package com.dremio.nessie.versioned.impl.experimental;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Map;

import com.dremio.nessie.jgit.GitStoreObjects;
import com.dremio.nessie.jgit.GitStoreObjects.Table.Builder;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.Contents;
import com.google.common.base.Joiner;
import com.google.protobuf.InvalidProtocolBufferException;

public class ProtoUtil {

  private static final Joiner DOT = Joiner.on('.');

  /**
   * turn a {@link com.dremio.nessie.model.Contents} into a protobuf object.
   *
   * @param table Table to persist
   * @param id optional ObjectId of the metadata
   * @return protobuf container
   */
  public static GitStoreObjects.Table tableToProtoc(Contents table, String id) {
    Builder builder = GitStoreObjects.Table.newBuilder()
                                           .setPath(table.getHash())
                                           .setMetadataLocation(table.getMetadataLocation());
    if (id != null) {
      builder.setMetadata(id);
    }
    return builder.build();
  }

  /**
   * Turn a protobuf serialized buffer into a Table object.
   *
   * @param data raw bytes
   * @return Table object and optionally the reference to the metadata object
   */
  public static Map.Entry<Contents, String> tableFromBytes(byte[] data) {
    try {
      GitStoreObjects.Table table = GitStoreObjects.Table.parseFrom(data);
      String[] names = table.getPath().split("\\.");
      String namespace = null;
      if (names.length > 1) {
        namespace = DOT.join(Arrays.copyOf(names, names.length - 1));
      }
      String name = names[names.length - 1];
      return new SimpleImmutableEntry<>(ImmutableTable.builder()
                                                      .id(table.getPath())
                                                      .metadataLocation(table.getMetadataLocation())
                                                      .name(name)
                                                      .namespace(namespace)
                                                      .build(),
                                        table.getMetadata());

    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
