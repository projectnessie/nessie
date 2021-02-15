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
package org.projectnessie.api;

public final class NessieHeaders {
  public static final String MESSAGE_HEADER_NAME = "Nessie-Commit-Message";
  public static final String AUTHOR_HEADER_NAME = "Nessie-Commit-Author";
  public static final String AUTHOR_DATE_HEADER_NAME = "Nessie-Commit-Author-Date";
  public static final String AUTHOR_EMAIL_HEADER_NAME = "Nessie-Commit-Author-Email";
  public static final String SIGNED_OFF_HEADER_NAME = "Nessie-Commit-Signed-Off";
  public static final String SIGNED_OFF_EMAIL_HEADER_NAME = "Nessie-Commit-Signed-Off-Email";
  public static final String SIGNED_OFF_DATE_HEADER_NAME = "Nessie-Commit-Signed-Off-Date";

  private NessieHeaders() {

  }
}
