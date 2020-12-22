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
package com.dremio.nessie.versioned.impl.condition;

/**
 * Visitor for all classes in the Value hierarchy.
 * @param <T> The type to which the Value will be converted.
 */
public interface ValueVisitor<T> {
  /**
   * Visit the passed in Value.
   * @param value the Value to visit.
   * @return the possibly transformed value resulting from the visitation.
   */
  T visit(Value value);

  /**
   * Visit the passed in ExpressionFunction.
   * @param value the ExpressionFunction to visit.
   * @return the possibly transformed value resulting from the visitation.
   */
  T visit(ExpressionFunction value);

  /**
   * Visit the passed in ExpressionPath.
   * @param value the ExpressionPath to visit.
   * @return the possibly transformed value resulting from the visitation.
   */
  T visit(ExpressionPath value);
}
