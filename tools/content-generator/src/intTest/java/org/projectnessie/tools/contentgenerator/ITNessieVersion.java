/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.tools.contentgenerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.tools.contentgenerator.RunContentGenerator.runGeneratorCmd;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.projectnessie.tools.contentgenerator.RunContentGenerator.ProcessResult;

class ITNessieVersion extends AbstractContentGeneratorTest {

  @Test
  void nessieVersion() {
    ProcessResult proc = runGeneratorCmd("-V");
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();
    assertThat(output)
        .hasSize(1)
        .anySatisfy(
            s -> assertThat(s.trim()).isEqualTo(System.getProperty("expectedNessieVersion")));
  }
}
