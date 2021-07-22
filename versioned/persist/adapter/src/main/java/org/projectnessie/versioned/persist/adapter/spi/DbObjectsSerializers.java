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
package org.projectnessie.versioned.persist.adapter.spi;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.Deserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.Serializers;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableKey;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.ContentsId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;

/** Jackson (de)serializers needed by {@link DatabaseAdapter} implementations. */
public class DbObjectsSerializers extends Serializers.Base {

  private static final JsonSerializer<ContentsId> CONTENTS_ID_SERIALIZER =
      new JsonSerializer<ContentsId>() {
        @Override
        public void serialize(ContentsId value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {
          gen.writeString(value.getId());
        }
      };

  private static final JsonDeserializer<ContentsId> CONTENTS_ID_DESERIALIZER =
      new JsonDeserializer<ContentsId>() {
        @Override
        public ContentsId deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException {
          return ContentsId.of(p.getValueAsString());
        }
      };

  private static final JsonSerializer<Hash> HASH_SERIALIZER =
      new JsonSerializer<Hash>() {
        @Override
        public void serialize(Hash value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {
          gen.writeBinary(value.asBytes().toByteArray());
        }
      };

  private static final JsonDeserializer<Hash> HASH_DESERIALIZER =
      new JsonDeserializer<Hash>() {
        @Override
        public Hash deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
          byte[] bytes = p.getBinaryValue();
          return Hash.of(UnsafeByteOperations.unsafeWrap(bytes));
        }
      };

  private static final JsonSerializer<NamedRef> NAMED_REF_SERIALIZER =
      new JsonSerializer<NamedRef>() {
        @Override
        public void serialize(NamedRef value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {
          if (value instanceof TagName) {
            gen.writeString("t_" + value.getName());
          } else if (value instanceof BranchName) {
            gen.writeString("b_" + value.getName());
          }
        }
      };

  private static final JsonDeserializer<NamedRef> NAMED_REF_DESERIALIZER =
      new JsonDeserializer<NamedRef>() {
        @Override
        public NamedRef deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
          String s = p.getValueAsString();
          if (s.startsWith("b_")) {
            return BranchName.of(s.substring(2));
          } else if (s.startsWith("t_")) {
            return TagName.of(s.substring(2));
          }
          throw new IllegalArgumentException(s);
        }
      };

  private static final JsonSerializer<Key> KEY_SERIALIZER =
      new JsonSerializer<Key>() {
        @Override
        public void serialize(Key value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {
          gen.writeString(flattenKey(value));
        }
      };

  private static final JsonDeserializer<Key> KEY_DESERIALIZER =
      new JsonDeserializer<Key>() {
        @Override
        public Key deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
          return fromFlattened(p.getValueAsString());
        }
      };

  private static final JsonSerializer<ByteString> BYTE_STRING_SERIALIZER =
      new JsonSerializer<ByteString>() {
        @Override
        public void serialize(ByteString value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {
          gen.writeBinary(value.toByteArray());
        }
      };

  private static final JsonDeserializer<ByteString> BYTE_STRING_DESERIALIZER =
      new JsonDeserializer<ByteString>() {
        @Override
        public ByteString deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException {
          byte[] bytes = p.getBinaryValue();
          return UnsafeByteOperations.unsafeWrap(bytes);
        }
      };

  public static ObjectMapper register(ObjectMapper mapper) {
    Deserializers deserializers =
        new Deserializers.Base() {
          @Override
          public JsonDeserializer<?> findBeanDeserializer(
              JavaType type, DeserializationConfig config, BeanDescription beanDesc) {
            if (type.isTypeOrSubTypeOf(Hash.class)) {
              return HASH_DESERIALIZER;
            }
            if (type.isTypeOrSubTypeOf(ContentsId.class)) {
              return CONTENTS_ID_DESERIALIZER;
            }
            if (type.isTypeOrSubTypeOf(ByteString.class)) {
              return BYTE_STRING_DESERIALIZER;
            }
            if (type.isTypeOrSubTypeOf(NamedRef.class)) {
              return NAMED_REF_DESERIALIZER;
            }
            if (type.isTypeOrSubTypeOf(Key.class)) {
              return KEY_DESERIALIZER;
            }
            return null;
          }
        };

    Serializers serializers =
        new Serializers.Base() {
          @Override
          public JsonSerializer<?> findSerializer(
              SerializationConfig config, JavaType type, BeanDescription beanDesc) {
            if (type.isTypeOrSubTypeOf(Hash.class)) {
              return HASH_SERIALIZER;
            }
            if (type.isTypeOrSubTypeOf(ContentsId.class)) {
              return CONTENTS_ID_SERIALIZER;
            }
            if (type.isTypeOrSubTypeOf(ByteString.class)) {
              return BYTE_STRING_SERIALIZER;
            }
            if (type.isTypeOrSubTypeOf(NamedRef.class)) {
              return NAMED_REF_SERIALIZER;
            }
            if (type.isTypeOrSubTypeOf(Key.class)) {
              return KEY_SERIALIZER;
            }
            return null;
          }
        };

    SimpleModule module =
        new SimpleModule() {
          @Override
          public void setupModule(SetupContext context) {
            super.setupModule(context);
            context.addDeserializers(deserializers);
            context.addSerializers(serializers);
          }
        };

    return mapper.registerModule(module);
  }

  /**
   * Convert a {@link #flattenKey(Key)} representation back into a {@link Key} object.
   *
   * @param flattened the {@link #flattenKey(Key)} representation
   * @return parsed {@link Key}
   */
  public static Key fromFlattened(String flattened) {
    ImmutableKey.Builder b = ImmutableKey.builder();
    StringBuilder sb = new StringBuilder();
    int l = flattened.length();
    for (int p = 0; p < l; p++) {
      char c = flattened.charAt(p);
      if (c == '\\') {
        c = flattened.charAt(++p);
        sb.append(c);
      } else if (c == '.') {
        if (sb.length() > 0) {
          b.addElements(sb.toString());
          sb.setLength(0);
        }
      } else {
        sb.append(c);
      }
    }
    if (sb.length() > 0) {
      b.addElements(sb.toString());
      sb.setLength(0);
    }
    return b.build();
  }

  /**
   * Flatten this {@link Key} into a single string.
   *
   * <p>Key elements are separated with a dot ({@code .}). Dots are escaped using a leading
   * backslash. Backslashes are escaped using two backslashes.
   *
   * @return the flattened representation
   * @see #fromFlattened(String)
   */
  public static String flattenKey(Key key) {
    StringBuilder sb = new StringBuilder();
    for (String element : key.getElements()) {
      if (sb.length() > 0) {
        sb.append('.');
      }

      int l = element.length();
      for (int i = 0; i < l; i++) {
        char c = element.charAt(i);
        switch (c) {
          case '.':
            sb.append("\\.");
            break;
          case '\\':
            sb.append("\\\\");
            break;
          default:
            sb.append(c);
            break;
        }
      }
    }
    return sb.toString();
  }
}
