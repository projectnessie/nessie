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
package org.projectnessie.restcatalog.api;

public class GenericIcebergRestException extends Exception {
  private final int responseCode;
  private final String type;

  public GenericIcebergRestException(int responseCode, String type, String message) {
    super(message);
    this.responseCode = responseCode;
    this.type = type;
  }

  public int getResponseCode() {
    return responseCode;
  }

  public String getType() {
    return type;
  }
}
