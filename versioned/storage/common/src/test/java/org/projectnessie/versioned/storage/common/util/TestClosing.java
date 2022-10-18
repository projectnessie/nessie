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
package org.projectnessie.versioned.storage.common.util;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.projectnessie.versioned.storage.common.util.Closing.closeMultiple;

import java.io.IOException;
import org.junit.jupiter.api.Test;

public class TestClosing {
  @SuppressWarnings("RedundantArrayCreation")
  @Test
  public void closeNothing() throws Exception {
    closeMultiple(null, null);
    closeMultiple(new AutoCloseable[0]);
    closeMultiple(emptyList());
    closeMultiple(asList(null, null));
  }

  @Test
  public void closeSomeArray() throws Exception {
    AutoCloseable closeable1 = mock(AutoCloseable.class);
    AutoCloseable closeable2 = mock(AutoCloseable.class);
    AutoCloseable closeable3 = mock(AutoCloseable.class);
    doNothing().when(closeable1).close();
    doNothing().when(closeable2).close();
    doNothing().when(closeable3).close();
    closeMultiple(closeable1, closeable2, closeable3);
    verify(closeable1, times(1)).close();
    verify(closeable2, times(1)).close();
    verify(closeable3, times(1)).close();
  }

  @Test
  public void closeSomeList() throws Exception {
    AutoCloseable closeable1 = mock(AutoCloseable.class);
    AutoCloseable closeable2 = mock(AutoCloseable.class);
    AutoCloseable closeable3 = mock(AutoCloseable.class);
    doNothing().when(closeable1).close();
    doNothing().when(closeable2).close();
    doNothing().when(closeable3).close();
    closeMultiple(asList(closeable1, closeable2, closeable3));
    verify(closeable1, times(1)).close();
    verify(closeable2, times(1)).close();
    verify(closeable3, times(1)).close();
  }

  @Test
  public void oneThrowing() throws Exception {
    AutoCloseable closeable1 = mock(AutoCloseable.class);
    AutoCloseable closeable2 = mock(AutoCloseable.class);
    AutoCloseable closeable3 = mock(AutoCloseable.class);
    doNothing().when(closeable1).close();
    doThrow(new IOException("closeable 2")).when(closeable2).close();
    doNothing().when(closeable3).close();
    assertThatThrownBy(() -> closeMultiple(closeable1, closeable2, closeable3))
        .isInstanceOf(IOException.class)
        .hasMessage("closeable 2");
    verify(closeable1, times(1)).close();
    verify(closeable2, times(1)).close();
    verify(closeable3, times(1)).close();
  }

  @Test
  public void firstTwoThrowing() throws Exception {
    AutoCloseable closeable1 = mock(AutoCloseable.class);
    AutoCloseable closeable2 = mock(AutoCloseable.class);
    AutoCloseable closeable3 = mock(AutoCloseable.class);
    doThrow(new IOException("closeable 1")).when(closeable1).close();
    IOException ex2 = new IOException("closeable 2");
    doThrow(ex2).when(closeable2).close();
    doNothing().when(closeable3).close();
    assertThatThrownBy(() -> closeMultiple(closeable1, closeable2, closeable3))
        .isInstanceOf(IOException.class)
        .hasMessage("closeable 1")
        .hasSuppressedException(ex2);
    verify(closeable1, times(1)).close();
    verify(closeable2, times(1)).close();
    verify(closeable3, times(1)).close();
  }

  @Test
  public void lastTwoThrowing() throws Exception {
    AutoCloseable closeable1 = mock(AutoCloseable.class);
    AutoCloseable closeable2 = mock(AutoCloseable.class);
    AutoCloseable closeable3 = mock(AutoCloseable.class);
    doNothing().when(closeable1).close();
    doThrow(new IOException("closeable 2")).when(closeable2).close();
    IOException ex3 = new IOException("closeable 3");
    doThrow(ex3).when(closeable3).close();
    assertThatThrownBy(() -> closeMultiple(closeable1, closeable2, closeable3))
        .isInstanceOf(IOException.class)
        .hasMessage("closeable 2")
        .hasSuppressedException(ex3);
    verify(closeable1, times(1)).close();
    verify(closeable2, times(1)).close();
    verify(closeable3, times(1)).close();
  }
}
