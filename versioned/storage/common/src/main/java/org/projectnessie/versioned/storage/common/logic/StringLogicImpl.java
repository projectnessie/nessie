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
package org.projectnessie.versioned.storage.common.logic;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.common.persist.ObjType.STRING;

import java.nio.charset.StandardCharsets;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;

final class StringLogicImpl implements StringLogic {
  private final Persist persist;

  StringLogicImpl(Persist persist) {
    this.persist = persist;
  }

  @Override
  public StringValue fetchString(ObjId stringObjId) throws ObjNotFoundException {
    return fetchString(persist.fetchTypedObj(stringObjId, STRING, StringObj.class));
  }

  @Override
  public StringValue fetchString(StringObj stringObj) {
    checkState(
        stringObj.compression() == Compression.NONE,
        "Unsupported compression %s",
        stringObj.compression());
    checkState(
        stringObj.predecessors().isEmpty(), "Predecessors in StringObj are not yet supported");

    return new StringValueHolder(stringObj);
  }

  @Override
  public StringObj updateString(
      StringValue previousValue, String contentType, byte[] stringValueUtf8) {
    // We currently ignore the previousValue here. It can be _later_ used to potentially store
    // diffs.

    ByteString text = unsafeWrap(stringValueUtf8);
    return stringData(contentType, Compression.NONE, null, emptyList(), text);
  }

  @Override
  public StringObj updateString(StringValue previousValue, String contentType, String stringValue) {
    return updateString(previousValue, contentType, stringValue.getBytes(StandardCharsets.UTF_8));
  }

  static final class StringValueHolder implements StringValue {
    final StringObj obj;

    StringValueHolder(StringObj obj) {
      this.obj = obj;
    }

    @Override
    public String contentType() {
      return obj.contentType();
    }

    @Override
    public String completeValue() throws ObjNotFoundException {
      return obj.text().toStringUtf8();
    }

    @Override
    public ObjId objId() {
      return obj.id();
    }
  }
}
