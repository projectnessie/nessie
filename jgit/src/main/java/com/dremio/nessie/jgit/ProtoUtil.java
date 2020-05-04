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

package com.dremio.nessie.jgit;

import com.dremio.nessie.jgit.GitStoreObjects.Table;
import com.dremio.nessie.model.BranchTable;
import com.dremio.nessie.model.ImmutableBranchTable;
import com.google.common.base.Joiner;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Arrays;

public class ProtoUtil {

  private static final Joiner DOT = Joiner.on('.');

  public static GitStoreObjects.Table tableToProtoc(BranchTable table) {
    return GitStoreObjects.Table.newBuilder()
                                .setPath(table.getId())
                                .setMetadataLocation(table.getMetadataLocation())
                                .setBaseLocation(table.getBaseLocation())
                                .build();
  }

  public static BranchTable tableFromBytes(byte[] data) {
    try {
      Table table = Table.parseFrom(data);
      String[] names = table.getPath().split("\\.");
      String namespace = null;
      if (names.length > 1) {
        namespace = DOT.join(Arrays.copyOf(names, names.length - 1));
      }
      String name = names[names.length - 1];
      return ImmutableBranchTable.builder()
                                 .id(table.getPath())
                                 .metadataLocation(table.getMetadataLocation())
                                 .tableName(name)
                                 .namespace(namespace)
                                 .baseLocation(table.getBaseLocation())
                                 .build();

    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

}
