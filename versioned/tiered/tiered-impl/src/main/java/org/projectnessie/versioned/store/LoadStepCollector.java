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
package org.projectnessie.versioned.store;

import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Stream;

class LoadStepCollector {
  static final Collector<LoadStep, StepCollectorState, LoadStep> COLLECTOR = Collector.of(
      StepCollectorState::new,
      StepCollectorState::plus,
      StepCollectorState::plus,
      StepCollectorState::getStep
  );

  private static final LoadStep EMPTY_STEP = new LoadStep() {
    @Override
    public Optional<LoadStep> getNext() {
      return Optional.empty();
    }

    @Override
    public Stream<LoadOp<?>> getOps() {
      return Stream.empty();
    }

    @Override
    public LoadStep combine(LoadStep other) {
      return other;
    }
  };

  private static class StepCollectorState {

    private LoadStep step = EMPTY_STEP;

    private StepCollectorState() {
    }

    public LoadStep getStep() {
      return step;
    }

    public StepCollectorState plus(StepCollectorState s) {
      plus(s.step);
      return this;
    }

    public void plus(LoadStep s) {
      step = step.combine(s);
    }
  }
}
