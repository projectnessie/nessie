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
package org.projectnessie.versioned.testworker;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.versioned.testworker.CommitMessage.commitMessage;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;
import static org.projectnessie.versioned.testworker.WithGlobalStateContent.withGlobal;

import com.google.protobuf.ByteString;
import java.util.Objects;
import java.util.function.Supplier;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.StoreWorker;

/**
 * {@link StoreWorker} implementation for tests using types that are independent of those in the
 * {@code nessie-model} Maven module.
 */
public final class SimpleStoreWorker
    implements StoreWorker<BaseContent, CommitMessage, BaseContent.Type> {

  public static final SimpleStoreWorker INSTANCE = new SimpleStoreWorker();

  private static final Serializer<CommitMessage> METADATA =
      new Serializer<CommitMessage>() {
        @Override
        public CommitMessage fromBytes(ByteString bytes) {
          return commitMessage(bytes.toString(UTF_8));
        }

        @Override
        public ByteString toBytes(CommitMessage value) {
          return ByteString.copyFromUtf8(value.getMessage());
        }
      };

  @Override
  public ByteString toStoreOnReferenceState(BaseContent content) {
    BaseContent.Type type = getType(content);
    String value;
    switch (type) {
      case ON_REF_ONLY:
        value = ((OnRefOnly) content).getOnRef();
        break;
      case WITH_GLOBAL_STATE:
        value = ((WithGlobalStateContent) content).getOnRef();
        break;
      default:
        throw new IllegalArgumentException("" + content);
    }
    return ByteString.copyFromUtf8(getType(content).name() + ":" + content.getId() + ":" + value);
  }

  @Override
  public ByteString toStoreGlobalState(BaseContent content) {
    if (content instanceof WithGlobalStateContent) {
      return ByteString.copyFromUtf8(((WithGlobalStateContent) content).getGlobal());
    }
    throw new IllegalArgumentException();
  }

  @Override
  public BaseContent valueFromStore(ByteString onReferenceValue, Supplier<ByteString> globalState) {
    String serialized = onReferenceValue.toStringUtf8();

    int i = serialized.indexOf(':');
    String typeString = serialized.substring(0, i);
    serialized = serialized.substring(i + 1);
    BaseContent.Type type = BaseContent.Type.valueOf(typeString);

    i = serialized.indexOf(':');
    String contentId = serialized.substring(0, i);
    i = serialized.indexOf(':');
    String onRef = serialized.substring(i + 1);

    ByteString global = globalState.get();
    switch (type) {
      case ON_REF_ONLY:
        assertThat(global).isNull();
        return onRef(onRef, contentId);
      case WITH_GLOBAL_STATE:
        assertThat(global).isNotNull();
        return withGlobal(global.toStringUtf8(), onRef, contentId);
      default:
        throw new IllegalArgumentException("" + onReferenceValue);
    }
  }

  @Override
  public BaseContent applyId(BaseContent baseContent, String id) {
    Objects.requireNonNull(baseContent, "baseContent must not be null");
    Objects.requireNonNull(id, "id must not be null");
    if (baseContent instanceof OnRefOnly) {
      OnRefOnly onRef = (OnRefOnly) baseContent;
      return OnRefOnly.onRef(onRef.getOnRef(), id);
    }
    if (baseContent instanceof WithGlobalStateContent) {
      WithGlobalStateContent withGlobal = (WithGlobalStateContent) baseContent;
      return WithGlobalStateContent.withGlobal(withGlobal.getGlobal(), withGlobal.getOnRef(), id);
    }
    throw new IllegalArgumentException("Unknown type " + baseContent);
  }

  @Override
  public String getId(BaseContent content) {
    return content.getId();
  }

  @Override
  public Byte getPayload(BaseContent content) {
    return (byte) getType(content).ordinal();
  }

  @Override
  public BaseContent.Type getType(ByteString onRefContent) {
    String serialized = onRefContent.toStringUtf8();
    int i = serialized.indexOf(':');
    if (i == -1) {
      return BaseContent.Type.ON_REF_ONLY;
    }
    String typeString = serialized.substring(0, i);
    return BaseContent.Type.valueOf(typeString);
  }

  @Override
  public BaseContent.Type getType(Byte payload) {
    return BaseContent.Type.values()[payload];
  }

  @Override
  public BaseContent.Type getType(BaseContent content) {
    if (content instanceof OnRefOnly) {
      return BaseContent.Type.ON_REF_ONLY;
    }
    if (content instanceof WithGlobalStateContent) {
      return BaseContent.Type.WITH_GLOBAL_STATE;
    }
    throw new IllegalArgumentException("" + content);
  }

  @Override
  public boolean requiresGlobalState(ByteString content) {
    return getType(content) == BaseContent.Type.WITH_GLOBAL_STATE;
  }

  @Override
  public boolean requiresGlobalState(BaseContent baseContent) {
    return baseContent instanceof WithGlobalStateContent;
  }

  @Override
  public Serializer<CommitMessage> getMetadataSerializer() {
    return METADATA;
  }
}
