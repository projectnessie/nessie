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
package org.projectnessie.versioned.gc;

import java.io.Serializable;

import com.google.protobuf.ByteString;

/**
 * Referenced state of a value, its type and its byte[] representation.
 */
public final class CategorizedValue implements Serializable {

  private static final long serialVersionUID = -1466847843373432962L;

  private boolean referenced;
  private byte[] data;
  private String entityType;
  private long timestamp;

  public CategorizedValue() {

  }

  /**
   * Construct asset key.
   */
  public CategorizedValue(boolean referenced, String valueType, ByteString data, long timestamp) {
    super();
    this.referenced = referenced;
    this.entityType = valueType;
    this.data = data.toByteArray();
    this.timestamp = timestamp;
  }

  public void setReferenced(boolean referenced) {
    this.referenced = referenced;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public boolean isReferenced() {
    return referenced;
  }

  public byte[] getData() {
    return data;
  }

  public String getEntityType() {
    return entityType;
  }

  public void setEntityType(String entityType) {
    this.entityType = entityType;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
}
