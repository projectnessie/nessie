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
package org.projectnessie.hms.annotation;

import com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public class MethodSignature {

  public static enum SigType {
    ORIG,
    ORIG_EXTEND,
    EXTENDED
  }

  private final MethodSignature.SigType type;
  private final Type returnType;
  private final Type[] argumentTypes;
  private final Type[] exceptionTypes;
  private final String name;

  /**
   * Create a method signature key based on a Reflection method signature.
   *
   * @param m The Reflect method to build from.
   */
  public MethodSignature(Method m) {
    super();
    this.returnType = m.getGenericReturnType();
    this.argumentTypes = m.getGenericParameterTypes();
    this.exceptionTypes = m.getGenericExceptionTypes();
    this.name = m.getName();
    this.type = hasCatalogExtend(m) ? SigType.ORIG_EXTEND : SigType.ORIG;
  }

  private MethodSignature(
      MethodSignature.SigType type,
      Type returnType,
      Type[] argumentTypes,
      Type[] exceptionTypes,
      String name) {
    super();
    this.type = type;
    this.returnType = returnType;
    this.argumentTypes = argumentTypes;
    this.exceptionTypes = exceptionTypes;
    this.name = name;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(argumentTypes);
    result = prime * result + Arrays.hashCode(exceptionTypes);
    result = prime * result + Objects.hash(name, returnType);
    return result;
  }

  private static boolean hasCatalogExtend(Method method) {
    return method.getAnnotation(CatalogExtend.class) != null;
  }

  /**
   * Extend this to a signature that includes catalog if appropriate.
   *
   * @return Extended signature with Catalog argument.
   */
  public Optional<MethodSignature> extendIfNecessary() {
    if (type != SigType.ORIG_EXTEND) {
      return Optional.empty();
    }

    Preconditions.checkArgument(type == SigType.ORIG_EXTEND);
    Type[] args = new Type[argumentTypes.length + 1];
    System.arraycopy(argumentTypes, 0, args, 1, argumentTypes.length);
    args[0] = String.class;
    return Optional.of(
        new MethodSignature(SigType.EXTENDED, returnType, args, exceptionTypes, name));
  }

  public boolean isExtended() {
    return type == SigType.EXTENDED;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MethodSignature)) {
      return false;
    }
    MethodSignature other = (MethodSignature) obj;
    return Arrays.equals(argumentTypes, other.argumentTypes)
        && Arrays.equals(exceptionTypes, other.exceptionTypes)
        && Objects.equals(name, other.name)
        && Objects.equals(returnType, other.returnType);
  }
}
