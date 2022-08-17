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

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import com.google.protobuf.ByteString;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.ContentAttachment;
import org.projectnessie.versioned.ContentAttachmentKey;

public class OnRefOnlySerializer extends TestContentSerializer<OnRefOnly> {

  @Override
  public Content.Type contentType() {
    return OnRefOnly.ON_REF_ONLY;
  }

  @Override
  public byte payload() {
    return 127;
  }

  @Override
  public ByteString toStoreOnReferenceState(
      OnRefOnly content, Consumer<ContentAttachment> attachmentConsumer) {
    return content.serialized();
  }

  @Override
  public OnRefOnly applyId(OnRefOnly content, String id) {
    return OnRefOnly.onRef(content.getOnRef(), id);
  }

  @Override
  protected OnRefOnly valueFromStore(
      String contentId,
      String onRef,
      ByteString global,
      Function<Stream<ContentAttachmentKey>, Stream<ContentAttachment>> attachmentsRetriever) {
    assertThat(global).isNull();
    return onRef(onRef, contentId);
  }
}
