/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.cache;

import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.commontests.AbstractBasePersistTests.randomContentId;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.commontests.AbstractPersistTests;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessiePersistCache;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@NessiePersistCache(enableSoftReferences = false)
public class TestCachingNoSoftReferencesInmemoryPersist extends AbstractPersistTests {

  @Nested
  @ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
  public class CacheSpecific {
    @InjectSoftAssertions protected SoftAssertions soft;

    @NessiePersist protected Persist persist;

    @Test
    public void getImmediate() throws Exception {
      Obj obj =
          contentValue(randomObjId(), 420L, randomContentId(), 1, ByteString.copyFromUtf8("hello"));
      soft.assertThat(persist.getImmediate(obj.id())).isNull();
      persist.storeObj(obj);
      soft.assertThat(persist.getImmediate(obj.id())).isEqualTo(obj);
      persist.deleteObj(obj.id());
      soft.assertThat(persist.getImmediate(obj.id())).isNull();
      persist.storeObj(obj);
      persist.fetchObj(obj.id());
      soft.assertThat(persist.getImmediate(obj.id())).isEqualTo(obj);
    }
  }
}
