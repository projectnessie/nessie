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
package org.projectnessie.tools.compatibility.internal;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.tools.compatibility.api.Version.NEW_STORAGE_MODEL_WITH_COMPAT_TESTING;
import static org.projectnessie.tools.compatibility.internal.Util.withClassLoader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.rest.NessieInternalServerException;
import org.projectnessie.client.rest.v1.HttpApiV1;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ErrorCodeAware;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieBackendThrottledException;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieForbiddenException;
import org.projectnessie.error.NessieReferenceAlreadyExistsException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.error.NessieRuntimeException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.tools.compatibility.api.Version;

@ExtendWith(SoftAssertionsExtension.class)
class TestTranslatingVersionNessieApi {
  @InjectSoftAssertions protected SoftAssertions soft;

  static ClassLoader oldVersionClassLoader;

  @BeforeAll
  static void init() throws Exception {
    oldVersionClassLoader =
        OldNessie.oldNessieClassLoader(
            NEW_STORAGE_MODEL_WITH_COMPAT_TESTING, singletonList("nessie-client"));
  }

  @Test
  void requiresProxyOrReserialization() {
    soft.assertThat(TranslatingVersionNessieApi.requiresProxy(null)).isFalse();
    soft.assertThat(TranslatingVersionNessieApi.requiresProxy(new ArrayList<>())).isFalse();
    soft.assertThat(TranslatingVersionNessieApi.requiresProxy(42)).isFalse();
    soft.assertThat(TranslatingVersionNessieApi.requiresProxy(new HttpApiV1(null))).isTrue();
    soft.assertThat(TranslatingVersionNessieApi.requiresProxy(Branch.of("foo", null))).isFalse();

    soft.assertThat(TranslatingVersionNessieApi.requiresReserialization(null)).isFalse();
    soft.assertThat(TranslatingVersionNessieApi.requiresReserialization(new ArrayList<>()))
        .isFalse();
    soft.assertThat(TranslatingVersionNessieApi.requiresReserialization(42)).isFalse();
    soft.assertThat(TranslatingVersionNessieApi.requiresReserialization(new HttpApiV1(null)))
        .isFalse();
    soft.assertThat(TranslatingVersionNessieApi.requiresReserialization(Branch.of("foo", null)))
        .isTrue();
  }

  static Stream<Arguments> translate() {
    return Stream.of(
        Arguments.of(ImmutableBranch.class.getName(), Branch.of("branch", null)),
        Arguments.of(
            ImmutableIcebergTable.class.getName(),
            IcebergTable.of("metadata", 42L, 43, 44, 45, "content-id")));
  }

  @ParameterizedTest
  @MethodSource("translate")
  void translate(String expectedClassName, Object modelObj) throws Exception {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

    try (TranslatingVersionNessieApi translating =
        new TranslatingVersionNessieApi(
            Version.CURRENT,
            createOldVersionNessieAPi(),
            NessieApiV1.class,
            oldVersionClassLoader)) {

      soft.assertThat(translating.translateObject(null, oldVersionClassLoader, contextClassLoader))
          .isNull();
      soft.assertThat(translating.translateArgs(null, oldVersionClassLoader, contextClassLoader))
          .isNull();
      soft.assertThat(
              translating.translateArgs(
                  new Object[] {null, null}, oldVersionClassLoader, contextClassLoader))
          .containsExactly(null, null);

      Object translatedObject =
          translating.translateObject(modelObj, oldVersionClassLoader, contextClassLoader);

      soft.assertThat(translatedObject)
          .extracting(Object::getClass)
          .matches(c -> c.getName().equals(expectedClassName))
          .matches(c -> c.getClassLoader() == oldVersionClassLoader);

      Object[] translatedArgs =
          translating.translateArgs(
              new Object[] {
                null,
                modelObj,
                singletonList(modelObj),
                Collections.singleton(modelObj),
                Collections.singletonMap("key", modelObj)
              },
              oldVersionClassLoader,
              contextClassLoader);
      soft.assertThat(translatedArgs).hasSize(5);
      soft.assertThat(translatedArgs[0]).isNull();
      soft.assertThat(translatedArgs[1])
          .extracting(Object::getClass)
          .matches(c -> c.getName().equals(expectedClassName))
          .matches(c -> c.getClassLoader() == oldVersionClassLoader);
      soft.assertThat(translatedArgs[2])
          .asInstanceOf(InstanceOfAssertFactories.list(Object.class))
          .hasSize(1)
          .allMatch(o -> o.getClass().getClassLoader() == oldVersionClassLoader);
      soft.assertThat(translatedArgs[3])
          .asInstanceOf(InstanceOfAssertFactories.collection(Object.class))
          .hasSize(1)
          .allMatch(o -> o.getClass().getClassLoader() == oldVersionClassLoader);
      soft.assertThat(translatedArgs[4])
          .asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
          .hasSize(1)
          .containsKey("key")
          .extractingByKey("key")
          .matches(o -> o.getClass().getClassLoader() == oldVersionClassLoader);
    }
  }

  static Stream<Arguments> serialization() {
    return Stream.of(
        Arguments.of(
            Branch.class,
            Branch.of("branch", null),
            "{\"type\":\"BRANCH\",\"name\":\"branch\",\"hash\":null}"),
        Arguments.of(
            IcebergTable.class,
            IcebergTable.of("metadata", 42L, 43, 44, 45, "content-id"),
            "{\"type\":\"ICEBERG_TABLE\",\"id\":\"content-id\",\"metadataLocation\":\"metadata\",\"snapshotId\":42,\"schemaId\":43,\"specId\":44,\"sortOrderId\":45}"));
  }

  @ParameterizedTest
  @MethodSource("serialization")
  void serialization(Class<?> modelClass, Object modelObj, String modelJson) throws Exception {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

    try (TranslatingVersionNessieApi translating =
        new TranslatingVersionNessieApi(
            Version.CURRENT,
            createOldVersionNessieAPi(),
            NessieApiV1.class,
            oldVersionClassLoader)) {

      Object translatedObject =
          translating.translateObject(modelObj, oldVersionClassLoader, contextClassLoader);

      soft.assertThat(translating.serializeWith(contextClassLoader, modelObj))
          .isEqualTo(translating.serializeWith(oldVersionClassLoader, translatedObject))
          .isEqualTo(modelJson);

      soft.assertThat(
              translating.deserializeWith(contextClassLoader, modelJson, modelClass.getName()))
          .isEqualTo(modelObj);
      soft.assertThat(
              translating.deserializeWith(oldVersionClassLoader, modelJson, modelClass.getName()))
          .isEqualTo(translatedObject);

      soft.assertThat(translating.reserialize(modelObj))
          .isEqualTo(translatedObject)
          .extracting(b -> b.getClass().getClassLoader())
          .isSameAs(oldVersionClassLoader);
      soft.assertThat(translating.reserialize(translatedObject))
          .isEqualTo(modelObj)
          .extracting(b -> b.getClass().getClassLoader())
          .isSameAs(contextClassLoader);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"org.projectnessie.model.Branch", "org.projectnessie.model.IcebergTable"})
  void translateTypes(String className) throws Exception {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

    try (TranslatingVersionNessieApi translating =
        new TranslatingVersionNessieApi(
            Version.CURRENT,
            createOldVersionNessieAPi(),
            NessieApiV1.class,
            oldVersionClassLoader)) {

      Class<?> contextClass = contextClassLoader.loadClass(className);

      Class<?>[] translated =
          translating.translateTypes(oldVersionClassLoader, new Class[] {contextClass});
      soft.assertThat(translated)
          .allSatisfy(c -> assertThat(c.getClassLoader()).isSameAs(oldVersionClassLoader));
      soft.assertThat(translating.translateTypes(oldVersionClassLoader, translated))
          .allSatisfy(c -> assertThat(c.getClassLoader()).isSameAs(oldVersionClassLoader));
      soft.assertThat(translating.translateTypes(contextClassLoader, translated))
          .allSatisfy(c -> assertThat(c.getClassLoader()).isSameAs(contextClassLoader));
    }
  }

  @Test
  void exceptionsInternalError() throws Exception {
    int statusCode = 500;
    String type = "internal error";
    String nessieErrorMessage = "Message " + type;
    String reason = "Reason " + type;
    String serverStackTrace = "Stack trace " + type;
    String exceptionMessage =
        String.format(
            "%s (HTTP/%d): %s\n%s", reason, statusCode, nessieErrorMessage, serverStackTrace);
    String oldVersionType = "org.projectnessie.client.rest.NessieInternalServerException";
    Class<? extends Throwable> expectedType = NessieInternalServerException.class;
    ErrorCode errorCode = ErrorCode.UNKNOWN;

    Throwable translated =
        translateException(
            oldVersionType, errorCode.httpStatus(), nessieErrorMessage, reason, serverStackTrace);

    soft.assertThat(translated)
        .isInstanceOf(expectedType)
        .asInstanceOf(InstanceOfAssertFactories.type(NessieInternalServerException.class))
        .extracting(Throwable::getMessage, NessieInternalServerException::getError)
        .containsExactly(
            exceptionMessage,
            ImmutableNessieError.builder()
                .reason(reason)
                .message(nessieErrorMessage)
                .status(statusCode)
                .errorCode(errorCode)
                .serverStackTrace(serverStackTrace)
                .build());
  }

  private static Stream<Arguments> runtimeExceptions() {
    // The "original" test code here used the exception classes in the package
    // org.projectnessie.client.rest, present last time in Nessie 0.19.0. Sadly, the pom of 0.19.0
    // has errors which prevent that version from being used with newer Maven/Maven-Resolver
    // versions - so the "current" and "old" class names are the same here, which might look odd.
    return Stream.of(
        Arguments.of(
            400,
            "bad request",
            NessieBackendThrottledException.class,
            "org.projectnessie.error.NessieBackendThrottledException",
            ErrorCode.UNKNOWN),
        Arguments.of(
            400,
            "bad request",
            NessieBadRequestException.class,
            "org.projectnessie.error.NessieBadRequestException",
            ErrorCode.UNKNOWN),
        Arguments.of(
            401,
            "forbidden",
            NessieForbiddenException.class,
            "org.projectnessie.error.NessieForbiddenException",
            ErrorCode.UNKNOWN));
  }

  @ParameterizedTest
  @MethodSource("runtimeExceptions")
  void runtimeExceptions(
      int statusCode,
      String type,
      Class<? extends Throwable> expectedType,
      String oldVersionType,
      ErrorCode errorCode)
      throws Exception {

    String nessieErrorMessage = "Message " + type;
    String exceptionMessage =
        String.format(
            "Reason %s (HTTP/%d): Message %s\n" + "Stack trace %s", type, statusCode, type, type);
    String reason = "Reason " + type;
    String serverStackTrace = "Stack trace " + type;

    Throwable translated =
        translateException(
            oldVersionType, statusCode, nessieErrorMessage, reason, serverStackTrace);
    soft.assertThat(translated)
        .isInstanceOf(expectedType)
        .asInstanceOf(InstanceOfAssertFactories.type(NessieRuntimeException.class))
        .extracting(
            Throwable::getMessage, ErrorCodeAware::getErrorCode, NessieRuntimeException::getError)
        .containsExactly(
            exceptionMessage,
            errorCode,
            ImmutableNessieError.builder()
                .reason(reason)
                .message(nessieErrorMessage)
                .status(statusCode)
                .errorCode(errorCode)
                .serverStackTrace(serverStackTrace)
                .build());
  }

  private static Stream<Arguments> baseExceptions() {
    return Stream.of(
        Arguments.of(
            NessieReferenceConflictException.class,
            "org.projectnessie.error.NessieReferenceConflictException",
            ErrorCode.REFERENCE_CONFLICT),
        Arguments.of(
            NessieReferenceAlreadyExistsException.class,
            "org.projectnessie.error.NessieReferenceAlreadyExistsException",
            ErrorCode.REFERENCE_ALREADY_EXISTS),
        Arguments.of(
            NessieContentNotFoundException.class,
            "org.projectnessie.error.NessieContentNotFoundException",
            ErrorCode.CONTENT_NOT_FOUND),
        Arguments.of(
            NessieReferenceNotFoundException.class,
            "org.projectnessie.error.NessieReferenceNotFoundException",
            ErrorCode.REFERENCE_NOT_FOUND));
  }

  @ParameterizedTest
  @MethodSource("baseExceptions")
  void baseExceptions(
      Class<? extends BaseNessieClientServerException> expectedType,
      String oldVersionType,
      ErrorCode errorCode)
      throws Exception {
    String message = "message " + errorCode.name();
    String reason = "reason " + errorCode.name();
    String serverStackTrace = "stack trace " + errorCode.name();

    Throwable translated =
        translateException(
            oldVersionType, errorCode.httpStatus(), message, reason, serverStackTrace);
    soft.assertThat(translated)
        .isInstanceOf(expectedType)
        .asInstanceOf(InstanceOfAssertFactories.type(BaseNessieClientServerException.class))
        .extracting(
            Throwable::getMessage,
            ErrorCodeAware::getErrorCode,
            BaseNessieClientServerException::getStatus,
            BaseNessieClientServerException::getServerStackTrace)
        .containsExactly(message, errorCode, errorCode.httpStatus(), serverStackTrace);
  }

  /**
   * Returns an exception from an "old" Nessie version translated to the current in-tree
   * representation.
   *
   * <p>Constructs a Nessie exception instance via {@code NessieError} from the classes in {@link
   * #oldVersionClassLoader} and returns it's the translated exception instance.
   */
  private static Throwable translateException(
      String oldVersionType, int statusCode, String message, String reason, String serverStackTrace)
      throws Exception {
    try (TranslatingVersionNessieApi translating =
        new TranslatingVersionNessieApi(
            Version.CURRENT,
            createOldVersionNessieAPi(),
            NessieApiV1.class,
            oldVersionClassLoader)) {
      Class<?> nessieErrorClass =
          oldVersionClassLoader.loadClass("org.projectnessie.error.NessieError");

      Object oldNessieError =
          withClassLoader(
              oldVersionClassLoader,
              () -> {
                Object builder =
                    oldVersionClassLoader
                        .loadClass("org.projectnessie.error.ImmutableNessieError")
                        .getDeclaredMethod("builder")
                        .invoke(null);
                Class<?> builderClass = builder.getClass();
                builderClass.getDeclaredMethod("status", int.class).invoke(builder, statusCode);
                builderClass.getDeclaredMethod("message", String.class).invoke(builder, message);
                builderClass.getDeclaredMethod("reason", String.class).invoke(builder, reason);
                builderClass
                    .getDeclaredMethod("serverStackTrace", String.class)
                    .invoke(builder, serverStackTrace);
                return builderClass.getDeclaredMethod("build").invoke(builder);
              });

      Throwable exception =
          oldVersionClassLoader
              .loadClass(oldVersionType)
              .asSubclass(Throwable.class)
              .getConstructor(nessieErrorClass)
              .newInstance(oldNessieError);
      return translating.translateException(exception);
    }
  }

  private static AutoCloseable createOldVersionNessieAPi() {
    return OldNessieApiHolder.createNessieClient(
        oldVersionClassLoader,
        new ClientKey(
            NEW_STORAGE_MODEL_WITH_COMPAT_TESTING,
            "org.projectnessie.client.http.HttpClientBuilder",
            NessieApiV1.class,
            Collections.singletonMap("nessie.uri", "http://127.42.42.42:19120")));
  }
}
