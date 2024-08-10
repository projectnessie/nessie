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
package org.projectnessie.client.api;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.MergeResponse;

/**
 * Request builder for "transplant commits".
 *
 * @since {@link NessieApiV1}
 */
public interface TransplantCommitsBuilder extends MergeTransplantBuilder<TransplantCommitsBuilder> {

  /**
   * Sets an override for the transplanted commit message. If an override is not set, messages from
   * the original commits are reused during transplanting.
   *
   * <p>Note: The message override is ignored when {@link #keepIndividualCommits(boolean) more than
   * one commit} is transplanted without squashing. In other words, the message override is
   * effective only when exactly one commit is produced on the target branch.
   */
  @Override
  TransplantCommitsBuilder message(String message);

  TransplantCommitsBuilder hashesToTransplant(
      @NotNull @Size(min = 1) List<String> hashesToTransplant);

  MergeResponse transplant() throws NessieNotFoundException, NessieConflictException;
}
