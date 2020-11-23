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
package com.dremio.nessie.versioned.store.mongodb.codecs;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;

import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.impl.InternalCommitMetadata;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.InternalValue;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;

/**
 * Codecs provide the heart of the SerDe process to/from BSON format.
 * The Codecs are inserted into a CodecRegistry. However they may require the CodecRegistry to do their job.
 * This apparent two way interdependency is resolved by using a CodecProvider.
 * The CodecProvider is a factory for Codecs.
 */
public class CodecProvider implements org.bson.codecs.configuration.CodecProvider {
  static class L1Codec extends BaseCodec<L1> {
    L1Codec() {
      super(L1.class, L1.SCHEMA);
    }
  }

  static class L2Codec extends BaseCodec<L2> {
    L2Codec() {
      super(L2.class, L2.SCHEMA);
    }
  }

  static class L3Codec extends BaseCodec<L3> {
    L3Codec() {
      super(L3.class, L3.SCHEMA);
    }
  }

  static class FragmentCodec extends BaseCodec<Fragment> {
    FragmentCodec() {
      super(Fragment.class, Fragment.SCHEMA);
    }
  }

  static class MetadataCodec extends BaseCodec<InternalCommitMetadata> {
    MetadataCodec() {
      super(InternalCommitMetadata.class, InternalCommitMetadata.SCHEMA);
    }
  }

  static class RefCodec extends BaseCodec<InternalRef> {
    RefCodec() {
      super(InternalRef.class, InternalRef.SCHEMA);
    }
  }

  static class ValueCodec extends BaseCodec<InternalValue> {
    ValueCodec() {
      super(InternalValue.class, InternalValue.SCHEMA);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
    if (clazz == L1.class) {
      return (Codec<T>) new L1Codec();
    } else if (clazz == L2.class) {
      return (Codec<T>) new L2Codec();
    } else if (clazz == L3.class) {
      return (Codec<T>) new L3Codec();
    } else if (clazz == Fragment.class) {
      return (Codec<T>) new FragmentCodec();
    } else if (clazz == InternalRef.class) {
      return (Codec<T>) new RefCodec();
    } else if (clazz == InternalCommitMetadata.class) {
      return (Codec<T>) new MetadataCodec();
    } else if (clazz == InternalValue.class) {
      return (Codec<T>) new ValueCodec();
    }

    // CodecProvider returns null if it's not a provider for the requested Class
    return null;
  }
}
