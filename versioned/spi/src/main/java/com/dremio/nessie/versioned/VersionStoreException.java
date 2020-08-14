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
package com.dremio.nessie.versioned;

/**
 * Base exception for exceptions generated by the version store api.
 *
 */
public class VersionStoreException extends Exception {
  private static final long serialVersionUID = 634125400451805341L;

  public VersionStoreException() {
    super();
  }

  public VersionStoreException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public VersionStoreException(String message, Throwable cause) {
    super(message, cause);
  }

  public VersionStoreException(String message) {
    super(message);
  }

  public VersionStoreException(Throwable cause) {
    super(cause);
  }
}
