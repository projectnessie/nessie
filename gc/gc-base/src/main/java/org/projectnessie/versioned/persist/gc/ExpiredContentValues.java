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
package org.projectnessie.versioned.persist.gc;

import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

/**
 * Contains details about one content object: reference, key, the live values and the expired
 * values.
 */
public abstract class ExpiredContentValues {

  private Reference reference;

  private ContentKey key;

  public ContentKey getKey() {
    return key;
  }

  public Reference getReference() {
    return reference;
  }

  private void setIfAbsent(Reference reference, ContentKey key) {
    if (this.reference == null) {
      this.reference = reference;
      this.key = key;
    }
  }

  void gotValue(Content content, Reference reference, ContentKey key, boolean isExpired) {
    synchronized (this) {
      addValue(content, isExpired);
      setIfAbsent(reference, key);
    }
  }

  protected abstract void addValue(Content content, boolean isExpired);

  @Override
  public String toString() {
    return "ExpiredContentValues{" + ", Reference=" + reference + ", Key=" + key + '}';
  }
}
