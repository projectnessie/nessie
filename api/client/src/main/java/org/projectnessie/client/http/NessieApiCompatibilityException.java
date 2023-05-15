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
package org.projectnessie.client.http;

public class NessieApiCompatibilityException extends RuntimeException {

  private final int clientApiVersion;
  private final int minServerApiVersion;
  private final int maxServerApiVersion;
  private final int actualServerApiVersion;

  public NessieApiCompatibilityException(
      int clientApiVersion,
      int minServerApiVersion,
      int maxServerApiVersion,
      int actualServerApiVersion) {
    super(
        formatMessage(
            clientApiVersion, minServerApiVersion, maxServerApiVersion, actualServerApiVersion));
    this.clientApiVersion = clientApiVersion;
    this.minServerApiVersion = minServerApiVersion;
    this.maxServerApiVersion = maxServerApiVersion;
    this.actualServerApiVersion = actualServerApiVersion;
  }

  private static String formatMessage(
      int clientApiVersion,
      int minServerApiVersion,
      int maxServerApiVersion,
      int actualServerApiVersion) {
    if (clientApiVersion < minServerApiVersion) {
      return String.format(
          "API version %d is too old for server (minimum supported version is %d)",
          clientApiVersion, minServerApiVersion);
    }
    if (clientApiVersion > maxServerApiVersion) {
      return String.format(
          "API version %d is too new for server (maximum supported version is %d)",
          clientApiVersion, maxServerApiVersion);
    }
    return String.format(
        "API version mismatch, check URI prefix (expected: %d, actual: %d)",
        clientApiVersion, actualServerApiVersion);
  }

  /** The client's API version. */
  public int getClientApiVersion() {
    return clientApiVersion;
  }

  /** The minimum API version supported by the server. */
  public int getMinServerApiVersion() {
    return minServerApiVersion;
  }

  /** The maximum API version supported by the server. */
  public int getMaxServerApiVersion() {
    return maxServerApiVersion;
  }

  /**
   * The actual API version used by the server, or zero if the server does not report its actual API
   * version.
   */
  public int getActualServerApiVersion() {
    return actualServerApiVersion;
  }
}
