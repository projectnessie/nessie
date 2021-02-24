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
package org.projectnessie.versioned.impl.condition;

/**
 * Visitor for all classes in the UpdateClause hierarchy.
 * @param <T> The type to which the UpdateClause will be converted.
 */
public interface UpdateClauseVisitor<T> {
  /**
   * Visit the passed in RemoveClause.
   * @param clause the clause to visit.
   * @return the possibly transformed value resulting from the visitation.
   */
  T visit(RemoveClause clause);

  /**
   * Visit the passed in SetClause.
   * @param clause the clause to visit.
   * @return the possibly transformed value resulting from the visitation.
   */
  T visit(SetClause clause);
}
