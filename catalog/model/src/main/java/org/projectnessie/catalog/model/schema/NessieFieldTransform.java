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
package org.projectnessie.catalog.model.schema;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.projectnessie.catalog.model.id.Hashable;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.catalog.model.schema.types.NessieType;
import org.projectnessie.catalog.model.schema.types.NessieTypeSpec;

public interface NessieFieldTransform extends Hashable {

  static NessieFieldTransform fromString(String transform) {

    // See o.a.i.transforms.Transforms.fromString(java.lang.String)
    // See https://iceberg.apache.org/spec/#partition-transforms

    Matcher widthMatcher = TransformParser.PATTERN.matcher(transform);
    String name;
    boolean hasWidth = widthMatcher.matches();
    int parsedWidth = -1;
    if (hasWidth) {
      name = widthMatcher.group(1);
      parsedWidth = Integer.parseInt(widthMatcher.group(2));
    } else {
      name = transform;
    }

    switch (name.toLowerCase(Locale.ROOT)) {
      case "truncate":
        if (hasWidth) {
          return truncate(parsedWidth);
        }
        break;
      case "bucket":
        if (hasWidth) {
          return bucket(parsedWidth);
        }
        break;
      case "identity":
        return identity();
      case "year":
        return year();
      case "month":
        return month();
      case "day":
        return day();
      case "hour":
        return hour();
      case "void":
        return voidTransform();
      default:
        break;
    }

    return unknownTransform(transform);
  }

  static NessieFieldTransform truncate(int width) {
    return new TransformParser.Truncate(width);
  }

  static NessieFieldTransform bucket(int buckets) {
    return new TransformParser.Bucket(buckets);
  }

  static NessieFieldTransform identity() {
    return TransformParser.IDENTITY;
  }

  static NessieFieldTransform year() {
    return TransformParser.YEAR;
  }

  static NessieFieldTransform month() {
    return TransformParser.MONTH;
  }

  static NessieFieldTransform day() {
    return TransformParser.DAY;
  }

  static NessieFieldTransform hour() {
    return TransformParser.HOUR;
  }

  static NessieFieldTransform voidTransform() {
    return TransformParser.VOID;
  }

  NessieTypeSpec transformedType(NessieTypeSpec sourceType);

  static NessieFieldTransform unknownTransform(String transform) {
    return new TransformParser.Unknown(transform);
  }
}

@SuppressWarnings("OneTopLevelClass")
final class TransformParser {
  private TransformParser() {}

  static final Pattern PATTERN = Pattern.compile("(\\w+)\\[(\\d+)]");

  static final NessieFieldTransform IDENTITY = new Identity();
  static final NessieFieldTransform YEAR = new Year();
  static final NessieFieldTransform MONTH = new Month();
  static final NessieFieldTransform DAY = new Day();
  static final NessieFieldTransform HOUR = new Hour();
  static final NessieFieldTransform VOID = new Void();

  static final class Truncate implements NessieFieldTransform {
    private final int width;

    Truncate(int width) {
      this.width = width;
    }

    @Override
    public NessieTypeSpec transformedType(NessieTypeSpec sourceType) {
      return sourceType;
    }

    @Override
    public void hash(NessieIdHasher idHasher) {
      idHasher.hash("truncate").hash(width);
    }

    @Override
    public String toString() {
      return "truncate[" + width + "]";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Truncate truncate = (Truncate) o;

      return width == truncate.width;
    }

    @Override
    public int hashCode() {
      return width;
    }
  }

  static final class Bucket implements NessieFieldTransform {
    private final int buckets;

    Bucket(int buckets) {
      this.buckets = buckets;
    }

    @Override
    public NessieTypeSpec transformedType(NessieTypeSpec sourceType) {
      return NessieType.intType();
    }

    @Override
    public void hash(NessieIdHasher idHasher) {
      idHasher.hash("bucket").hash(buckets);
    }

    @Override
    public String toString() {
      return "bucket[" + buckets + "]";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Bucket bucket = (Bucket) o;

      return buckets == bucket.buckets;
    }

    @Override
    public int hashCode() {
      return buckets;
    }
  }

  static final class Identity implements NessieFieldTransform {
    @Override
    public String toString() {
      return "identity";
    }

    @Override
    public void hash(NessieIdHasher idHasher) {
      idHasher.hash("identity");
    }

    @Override
    public NessieTypeSpec transformedType(NessieTypeSpec sourceType) {
      return sourceType;
    }

    @Override
    public int hashCode() {
      return 1;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof Identity;
    }
  }

  static final class Year implements NessieFieldTransform {
    @Override
    public String toString() {
      return "year";
    }

    @Override
    public void hash(NessieIdHasher idHasher) {
      idHasher.hash("year");
    }

    @Override
    public NessieTypeSpec transformedType(NessieTypeSpec sourceType) {
      return NessieType.intType();
    }

    @Override
    public int hashCode() {
      return 2;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof Year;
    }
  }

  static final class Month implements NessieFieldTransform {
    @Override
    public String toString() {
      return "month";
    }

    @Override
    public void hash(NessieIdHasher idHasher) {
      idHasher.hash("month");
    }

    @Override
    public NessieTypeSpec transformedType(NessieTypeSpec sourceType) {
      return NessieType.intType();
    }

    @Override
    public int hashCode() {
      return 3;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof Month;
    }
  }

  static final class Day implements NessieFieldTransform {
    @Override
    public String toString() {
      return "day";
    }

    @Override
    public void hash(NessieIdHasher idHasher) {
      idHasher.hash("day");
    }

    @Override
    public NessieTypeSpec transformedType(NessieTypeSpec sourceType) {
      return NessieType.intType();
    }

    @Override
    public int hashCode() {
      return 4;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof Day;
    }
  }

  static final class Hour implements NessieFieldTransform {
    @Override
    public String toString() {
      return "hour";
    }

    @Override
    public void hash(NessieIdHasher idHasher) {
      idHasher.hash("hour");
    }

    @Override
    public NessieTypeSpec transformedType(NessieTypeSpec sourceType) {
      return NessieType.intType();
    }

    @Override
    public int hashCode() {
      return 5;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof Hour;
    }
  }

  static final class Void implements NessieFieldTransform {
    @Override
    public String toString() {
      return "void";
    }

    @Override
    public void hash(NessieIdHasher idHasher) {
      idHasher.hash("void");
    }

    @Override
    public NessieTypeSpec transformedType(NessieTypeSpec sourceType) {
      return sourceType;
    }

    @Override
    public int hashCode() {
      return 6;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof Void;
    }
  }

  static final class Unknown implements NessieFieldTransform {
    private final String transform;

    Unknown(String transform) {
      this.transform = transform;
    }

    @Override
    public String toString() {
      return transform;
    }

    @Override
    public void hash(NessieIdHasher idHasher) {
      idHasher.hash(transform);
    }

    @Override
    public NessieTypeSpec transformedType(NessieTypeSpec sourceType) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Unknown unknown = (Unknown) o;

      return Objects.equals(transform, unknown.transform);
    }

    @Override
    public int hashCode() {
      return transform != null ? transform.hashCode() : 0;
    }
  }
}
