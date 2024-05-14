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
package org.projectnessie.versioned.storage.common.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterOutputStream;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public final class Compressions {

  public static final int KEEP_UNCOMPRESSED = 8192;

  private Compressions() {}

  public static byte[] compressDefault(byte[] bytes, Consumer<Compression> compression) {
    if (bytes.length <= KEEP_UNCOMPRESSED) {
      compression.accept(Compression.NONE);
      return bytes;
    }
    Compression compr = Compression.GZIP;
    compression.accept(compr);
    return compress(compr, bytes);
  }

  public static byte[] compress(Compression compression, byte[] uncompressed) {
    switch (compression) {
      case NONE:
        return uncompressed;
      case GZIP:
        return gzip(uncompressed);
      case DEFLATE:
        return deflate(uncompressed);
      case SNAPPY:
        return snappyCompress(uncompressed);
      default:
        throw new IllegalArgumentException("Compression " + compression + " not implemented");
    }
  }

  public static byte[] uncompress(Compression compression, byte[] compressed) {
    switch (compression) {
      case NONE:
        return compressed;
      case GZIP:
        return gunzip(compressed);
      case DEFLATE:
        return inflate(compressed);
      case SNAPPY:
        return snappyUncompress(compressed);
      default:
        throw new IllegalArgumentException("Compression " + compression + " not implemented");
    }
  }

  private static byte[] gzip(byte[] uncompressed) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(uncompressed.length);
    try (OutputStream def = new GZIPOutputStream(out)) {
      def.write(uncompressed);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  private static byte[] gunzip(byte[] compressed) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(compressed.length * 2);
    try (InputStream gz = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
      gz.transferTo(out);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  private static byte[] deflate(byte[] uncompressed) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(uncompressed.length);
    try (OutputStream def = new DeflaterOutputStream(out, new Deflater(1))) {
      def.write(uncompressed);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  private static byte[] inflate(byte[] compressed) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(compressed.length * 2);
    try (OutputStream def = new InflaterOutputStream(out)) {
      def.write(compressed);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  private static byte[] snappyCompress(byte[] uncompressed) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(uncompressed.length);
    try (OutputStream def = new SnappyOutputStream(out)) {
      def.write(uncompressed);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  private static byte[] snappyUncompress(byte[] compressed) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(compressed.length * 2);
    try (InputStream input = new SnappyInputStream(new ByteArrayInputStream(compressed))) {
      input.transferTo(out);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }
}
