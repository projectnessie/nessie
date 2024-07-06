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
package org.projectnessie.versioned.storage.common.objtypes;

import java.util.function.LongSupplier;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public abstract class CustomObjType<T extends Obj> implements ObjType {

  private final String name;
  private final String shortName;
  private final Class<T> targetClass;

  private CustomObjType(String name, String shortName, Class<T> targetClass) {
    this.name = name;
    this.shortName = shortName;
    this.targetClass = targetClass;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String shortName() {
    return shortName;
  }

  @Override
  public Class<T> targetClass() {
    return targetClass;
  }

  public static <T extends Obj> ObjType customObjType(
      String name, String shortName, Class<T> targetClass) {
    return new CustomObjType<>(name, shortName, targetClass) {};
  }

  public static <T extends Obj> ObjType uncachedObjType(
      String name, String shortName, Class<T> targetClass) {
    return new CustomObjType<>(name, shortName, targetClass) {
      @Override
      public long cachedObjectExpiresAtMicros(Obj obj, LongSupplier clock) {
        return 0L;
      }
    };
  }

  @FunctionalInterface
  public interface CacheExpireCalculation<T extends Obj> {
    /**
     * See {@link ObjType#cachedObjectExpiresAtMicros(Obj, LongSupplier)} for the meaning of the
     * possible return values.
     */
    long calculateExpiresAt(T obj, long currentTimeMicros);
  }

  @FunctionalInterface
  public interface CacheNegativeCalculation {
    /**
     * See {@link ObjType#negativeCacheExpiresAtMicros(LongSupplier)} } for the meaning of the
     * possible return values.
     */
    long negativeCacheExpiresAtMicros(long currentTimeMicros);
  }

  public static <T extends Obj> ObjType dynamicCaching(
      String name,
      String shortName,
      Class<T> targetClass,
      CacheExpireCalculation<T> cacheExpireCalculation,
      CacheNegativeCalculation cacheNegativeCalculation) {
    return new CustomObjType<>(name, shortName, targetClass) {
      @Override
      public long cachedObjectExpiresAtMicros(Obj obj, LongSupplier clock) {
        @SuppressWarnings("unchecked")
        T casted = (T) obj;
        return cacheExpireCalculation.calculateExpiresAt(casted, clock.getAsLong());
      }

      @Override
      public long negativeCacheExpiresAtMicros(LongSupplier clock) {
        return cacheNegativeCalculation.negativeCacheExpiresAtMicros(clock.getAsLong());
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CustomObjType<?> that = (CustomObjType<?>) o;

    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return name;
  }
}
