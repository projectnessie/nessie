/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.server.catalog;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.quarkus.arc.Arc;
import io.quarkus.arc.ClientProxy;
import io.quarkus.arc.InjectableContext.ContextState;
import io.quarkus.arc.ManagedContext;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.service.objtypes.EntitySnapshotObj;
import org.projectnessie.nessie.tasks.api.TaskBehavior;
import org.projectnessie.nessie.tasks.api.TaskRequest;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.nessie.tasks.api.Tasks;
import org.projectnessie.nessie.tasks.api.TasksService;
import org.projectnessie.quarkus.tests.profiles.QuarkusTestProfilePersistInmemory;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;

@QuarkusTest
@TestProfile(QuarkusTestProfilePersistInmemory.class)
public class TestTaskRequestContext {
  @Inject TasksService tasksService;
  @Inject Persist persist;
  @Inject ObjectIO objectIO;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  public void sharedTaskUsesEachCallersContext(boolean fail) throws Exception {
    TaskBehavior<EntitySnapshotObj, EntitySnapshotObj.Builder> behavior = mock(TaskBehavior.class);
    when(behavior.objType()).thenReturn(EntitySnapshotObj.OBJ_TYPE);
    when(behavior.newObjBuilder()).thenAnswer(invocation -> EntitySnapshotObj.builder());
    when(behavior.runningTaskState(any(), any()))
        .thenAnswer(invocation -> {
          Clock clock = invocation.getArgument(0);
          return TaskState.runningState(
              clock.instant().plusSeconds(60), clock.instant().plusSeconds(120));
        });
    when(behavior.performRunningStateUpdateAt(any(), any()))
        .thenAnswer(invocation -> invocation.<Clock>getArgument(0).instant().plusSeconds(30));
    when(behavior.asErrorTaskState(any(), any(), any()))
        .thenAnswer(
            invocation -> TaskState.failureState(invocation.getArgument(2).toString(), null));

    CompletableFuture<EntitySnapshotObj.Builder> execution = new CompletableFuture<>();
    CountDownLatch started = new CountDownLatch(1);
    ObjId id = ObjId.randomObjId();
    TaskRequest<EntitySnapshotObj, EntitySnapshotObj.Builder> request = mock(TaskRequest.class);
    when(request.objId()).thenReturn(id);
    when(request.objType()).thenReturn(EntitySnapshotObj.OBJ_TYPE);
    when(request.behavior()).thenReturn(behavior);
    when(request.applyRequestToObjBuilder(any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    when(request.submitExecution())
        .thenAnswer(invocation -> {
          started.countDown();
          return execution;
        });

    Tasks tasks = tasksService.forPersist(persist);
    ManagedContext context = Arc.container().requestContext();
    ContextState original = context.isActive() ? context.getState() : null;
    context.deactivate();
    ContextState first = null;
    ContextState second = null;
    try {
      context.activate();
      first = context.getState();
      ObjectIO firstIO = ClientProxy.unwrap(objectIO);
      CompletionStage<ObjectIO> firstResult =
          tasks.submit(request).handle((result, failure) -> ClientProxy.unwrap(objectIO));
      assertThat(started.await(10, SECONDS)).isTrue();
      context.deactivate();

      context.activate();
      second = context.getState();
      ObjectIO secondIO = ClientProxy.unwrap(objectIO);
      assertThat(secondIO).isNotSameAs(firstIO);
      CompletableFuture<Void> continueSecond = new CompletableFuture<>();
      CompletionStage<ObjectIO> secondResult =
          tasks
              .submit(request)
              .handle((result, failure) -> {
                assertThat(failure != null).isEqualTo(fail);
                assertThat(ClientProxy.unwrap(objectIO)).isSameAs(secondIO);
                return result;
              })
              .thenCompose(result -> continueSecond)
              .thenApply(ignored -> ClientProxy.unwrap(objectIO));
      context.deactivate();

      // Complete both callers' shared task under the first request's context.
      context.activate(first);
      if (fail) {
        execution.completeExceptionally(new IllegalStateException("task failure"));
      } else {
        execution.complete(EntitySnapshotObj.builder().id(id).taskState(TaskState.successState()));
      }
      assertThat(firstResult.toCompletableFuture().get(10, SECONDS)).isSameAs(firstIO);
      context.terminate();
      first = null;

      // The second request is still alive after the originating request has ended. Its
      // subsequent continuations must also restore its own context, even on another context.
      continueSecond.complete(null);
      assertThat(secondResult.toCompletableFuture().get(10, SECONDS)).isSameAs(secondIO);
      assertThat(context.isActive()).isFalse();
      verify(request, times(1)).submitExecution();
    } finally {
      execution.completeExceptionally(new IllegalStateException("test finished"));
      context.deactivate();
      if (first != null) {
        context.activate(first);
        context.terminate();
      }
      if (second != null) {
        context.activate(second);
        context.terminate();
      }
      if (original != null) {
        context.activate(original);
      }
    }
  }
}
