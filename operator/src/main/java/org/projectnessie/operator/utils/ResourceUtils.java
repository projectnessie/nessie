/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.operator.utils;

import org.projectnessie.operator.events.EventReason;
import org.projectnessie.operator.exception.InvalidSpecException;

public final class ResourceUtils {

  private static final int MAX_DNS_LABEL_LENGTH = 63;

  private ResourceUtils() {}

  public static void validateName(String name) {
    validateName(name, MAX_DNS_LABEL_LENGTH);
  }

  /**
   * Validates that the given name is a valid DNS label according to RFC 1035 (which is more
   * restrictive than RFC 1123).
   *
   * @param name the name to validate
   * @param maxLength the maximum length of the name, which is 63 by default
   */
  public static void validateName(String name, int maxLength) {
    if (name == null || name.isEmpty()) {
      throw new InvalidSpecException(
          EventReason.InvalidName, "Resource name cannot be null or empty");
    }
    if (name.length() > maxLength) {
      throw new InvalidSpecException(
          EventReason.InvalidName,
          "Resource name cannot be longer than " + maxLength + " characters");
    }
    if (!name.matches("[a-z]([-a-z0-9]*[a-z0-9])?")) {
      throw new InvalidSpecException(
          EventReason.InvalidName,
          "Resource name must consist of lower case alphanumeric characters or '-', "
              + "start with an alphabetic character, "
              + "and end with an alphanumeric character");
    }
  }
}
