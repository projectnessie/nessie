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
package org.projectnessie.versioned;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.google.common.io.BaseEncoding;
import com.google.common.io.BaseEncoding.DecodingException;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Describes a specific point in time/history. Internally is a binary value but a string
 * representation is available.
 */
public final class Hash implements Ref {

  private static final BaseEncoding ENCODING = BaseEncoding.base16().lowerCase();
  private final ByteString bytes;

  private Hash(ByteString bytes) {
    this.bytes = requireNonNull(bytes);
  }

  /** Generates a string representation of the hash suitable to be used with {@link #of(String)}. */
  public String asString() {
    final int maxSize = 2 * bytes.size();
    try (final StringWriter sw = new StringWriter(maxSize);
        final OutputStream os = ENCODING.encodingStream(sw)) {
      bytes.writeTo(os);
      return sw.toString();
    } catch (IOException e) {
      // Should not happen as all operations are in memory
      throw new AssertionError(e);
    }
  }

  /**
   * Gets the bytes representation of the hash.
   *
   * @return the hash's bytes
   */
  public ByteString asBytes() {
    return bytes;
  }

  /**
   * Creates a hash instance from its string representation.
   *
   * @param hash the string representation of the hash
   * @return a {@code Hash} instance
   * @throws IllegalArgumentException if {@code hash} is not a valid representation of a hash
   * @throws NullPointerException if {@code hash} is {@code null}
   */
  public static Hash of(@Nonnull String hash) {
    requireNonNull(hash);
    checkArgument(
        hash.length() % 2 == 0, "hash length needs to be a multiple of two, was %s", hash.length());

    final int maxSize = hash.length() / 2;
    try (final StringReader sr = new StringReader(hash);
        final InputStream is = ENCODING.decodingStream(sr)) {
      ByteString bytes = ByteString.readFrom(is, maxSize);
      return Hash.of(bytes);
    } catch (DecodingException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      // Should not happen as all operations are in memory
      throw new AssertionError(e);
    }
  }

  /**
   * Creates a hash instance from its bytes' representation.
   *
   * @param bytes the bytes' representation of the hash
   * @return a {@code Hash} instance
   * @throws NullPointerException if {@code hash} is {@code null}
   */
  public static Hash of(@Nonnull ByteString bytes) {
    return new Hash(bytes);
  }

  @Override
  public int hashCode() {
    return bytes.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Hash)) {
      return false;
    }
    Hash that = (Hash) obj;
    return Objects.equals(this.bytes, that.bytes);
  }

  @Override
  public final String toString() {
    return "Hash " + asString();
  }
}
