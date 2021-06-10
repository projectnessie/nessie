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
package org.projectnessie.hms;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.lang.model.element.Modifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.projectnessie.hms.annotation.MethodSignature;
import org.projectnessie.hms.annotation.NoopQuiet;
import org.projectnessie.hms.annotation.NoopQuiet.QuietMode;
import org.projectnessie.hms.annotation.NoopThrow;
import org.projectnessie.hms.annotation.Route;
import org.projectnessie.hms.annotation.Union;
import org.projectnessie.hms.apis.AnnotatedHive2RawStore;
import org.projectnessie.hms.apis.AnnotatedHive3RawStore;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "generate-hive-raw-store",
    mixinStandardHelpOptions = true,
    description =
        "Generates delegating and non-delegating RawStore implementations for a particular Hive version.")
public class CodeGenerator implements Callable<Integer> {

  @Option(
      names = {"-m", "--mode"},
      required = true,
      description = "Hive version to generate. Valid values: ${COMPLETION-CANDIDATES}")
  private HiveVersion version = HiveVersion.HIVE3;

  @Option(
      names = {"-o", "--output"},
      required = true,
      description = "Output path to write to")
  private String outputPath;

  @Override
  public Integer call() throws Exception {
    TypeSpec.Builder raw = version.nonDelegatingBuilder();

    Map<MethodSignature, MethodHolder> methods = new HashMap<>();

    for (Method m : version.annotatedInterface.getMethods()) {
      MethodSignature s = new MethodSignature(m);
      methods.put(s, new MethodHolder(m, false));
      s.extendIfNecessary().ifPresent(s2 -> methods.put(s2, new MethodHolder(m, true)));
    }

    for (Method definedMethod : RawStore.class.getMethods()) {
      final MethodSignature signature = new MethodSignature(definedMethod);
      final MethodHolder holder = methods.get(signature);
      if (holder == null) {
        throw new IllegalStateException("unknown method: " + definedMethod);
      }

      generateMethod(version, holder, definedMethod).ifPresent(raw::addMethod);
    }

    raw.addMethod(
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addStatement("super(false)")
            .build());

    raw.addMethod(
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(boolean.class, "delegate")
            .addStatement("super(delegate)")
            .build());

    TypeSpec nodelegate = raw.build();
    {
      JavaFile javaFile = JavaFile.builder("org.projectnessie", nodelegate).build();
      javaFile.writeTo(new File(outputPath));
    }

    TypeSpec.Builder delegating = version.delegatingBuilder(nodelegate);

    delegating.addMethod(
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addStatement("super(true)")
            .build());
    JavaFile javaFile = JavaFile.builder("org.projectnessie", delegating.build()).build();
    javaFile.writeTo(new File(outputPath));

    return 0;
  }

  private static Optional<MethodSpec> generateMethod(
      HiveVersion version, MethodHolder holder, Method definedMethod) {

    final Method boundMethod = holder.method;
    final boolean extended = holder.extended();

    final NoopQuiet quiet = boundMethod.getAnnotation(NoopQuiet.class);
    final NoopThrow loud = boundMethod.getAnnotation(NoopThrow.class);
    final Union union = boundMethod.getAnnotation(Union.class);

    List<Param> parameters =
        Stream.of(definedMethod.getParameters())
            .map(p -> new Param(p.getName(), p.getParameterizedType()))
            .collect(Collectors.toList());
    if (extended) {
      parameters.set(0, new Param("catalogName", parameters.get(0).getType()));
      for (int i = 0; i < boundMethod.getParameterCount(); i++) {
        Parameter p = boundMethod.getParameters()[i];
        parameters.set(i + 1, new Param(p.getName(), p.getParameterizedType()));
      }
    } else {
      for (int i = 0; i < boundMethod.getParameterCount(); i++) {
        Parameter p = boundMethod.getParameters()[i];
        parameters.set(i, new Param(p.getName(), p.getParameterizedType()));
      }
    }

    int argNumber = -1;
    for (int i = 0; i < boundMethod.getParameterCount(); i++) {
      Parameter p = boundMethod.getParameters()[i];
      Route anno = p.getAnnotation(Route.class);
      if (anno != null) {
        if (holder.extended) {
          argNumber = i + 1;
        } else {
          argNumber = i;
        }
        break;
      }
    }

    boolean routed = argNumber != -1;

    boolean noAnnotation = quiet == null && loud == null && union == null;

    if (!routed && noAnnotation) {
      // don't delegate.
      return Optional.empty();
    }

    Param param = routed ? parameters.get(argNumber) : null;
    boolean hasReturn = definedMethod.getGenericReturnType() != void.class;

    MethodSpec.Builder meth =
        MethodSpec.methodBuilder(definedMethod.getName())
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Override.class)
            .returns(definedMethod.getGenericReturnType());

    if (routed) {
      meth.addComment("This method is routed based on the database name provided.");
      if (quiet != null) {
        meth.addComment("If the routing targets nessie, this is a noop.");
      } else if (loud != null) {
        meth.addComment("If the routing targets nessie, this will throw an exception.");
      }
    } else {
      meth.addComment("This method uses a delegate if it exists.");
      if (quiet != null) {
        meth.addComment("If no delegate exists, this method is a noop.");
      } else if (loud != null) {
        meth.addComment("If no delegate exists, this method will throw an exception.");
      } else if (union != null) {
        meth.addComment("Union of implementation results is generated.");
      }
    }

    String returnStr = hasReturn ? "return " : "";

    List<Param> nessieList =
        holder.extended ? parameters.subList(1, parameters.size()) : parameters;
    parameters.stream().forEach(p -> meth.addParameter(p.getType(), p.getName()));
    Stream.of(definedMethod.getGenericExceptionTypes()).forEach(e -> meth.addException(e));

    if (routed) {
      CodeBlock.Builder code = CodeBlock.builder();

      if (param.getType() instanceof ParameterizedType
          && ((ParameterizedType) param.getType()).getRawType() == List.class) {
        code.beginControlFlow(
            "if (routeToDelegate($L.stream().flatMap($T::route)))",
            param.getName(),
            version.baseRawStore);
      } else {
        code.beginControlFlow("if (routeToDelegate(route($L)))", param.getName());
      }

      code.addStatement(
          "$Ldelegate.$L($L)",
          returnStr,
          definedMethod.getName(),
          parameters.stream().map(Param::getName).collect(Collectors.joining(", ")));
      code.nextControlFlow("else");
      if (noAnnotation) {
        code.addStatement(
            "$Lnessie.$L($L)",
            returnStr,
            definedMethod.getName(),
            nessieList.stream().map(Param::getName).collect(Collectors.joining(", ")));
      }

      if (quiet != null) {
        quietReturn(version, code, quiet, definedMethod.getReturnType());
      } else if (loud != null) {
        code.addStatement("throw new IllegalArgumentException(\"Loud Failure\")");
      }

      code.endControlFlow();
      meth.addCode(code.build());

    } else {

      if (union == null) {

        if (quiet != null) {
          CodeBlock.Builder code = CodeBlock.builder();
          code.beginControlFlow("if (hasDelegate)");
          code.addStatement(
              "$Ldelegate.$L($L)",
              returnStr,
              definedMethod.getName(),
              parameters.stream().map(Param::getName).collect(Collectors.joining(", ")));
          code.nextControlFlow("else");
          quietReturn(version, code, quiet, definedMethod.getReturnType());
          code.endControlFlow();
          meth.addCode(code.build());
        } else {
          meth.addStatement("checkHasDelegate()");
          meth.addStatement(
              "$Ldelegate.$L($L)",
              returnStr,
              definedMethod.getName(),
              parameters.stream().map(Param::getName).collect(Collectors.joining(", ")));
        }

      } else {
        CodeBlock.Builder b = CodeBlock.builder();
        b.beginControlFlow("if (hasDelegate)");
        b.addStatement(
            "return union(delegate.$L($L), nessie.$L($L))",
            definedMethod.getName(),
            parameters.stream().map(Param::getName).collect(Collectors.joining(", ")),
            definedMethod.getName(),
            nessieList.stream().map(Param::getName).collect(Collectors.joining(", ")));
        b.nextControlFlow("else");
        b.addStatement(
            "return nessie.$L($L)",
            definedMethod.getName(),
            nessieList.stream().map(Param::getName).collect(Collectors.joining(", ")));
        b.endControlFlow();
        meth.addCode(b.build());
      }
    }

    return Optional.of(meth.build());
  }

  private static void quietReturn(
      HiveVersion version, CodeBlock.Builder code, NoopQuiet quiet, Class<?> returnType) {
    if (returnType != void.class) {
      if (quiet.value() == QuietMode.NULL) {
        code.addStatement("return null");
      } else if (returnType == List.class) {
        code.addStatement("return $T.emptyList()", Collections.class);
      } else if (returnType == long.class) {
        code.addStatement("return 0L");
      } else if (returnType == boolean.class) {
        code.addStatement("return true");
      } else if (returnType == PrincipalPrivilegeSet.class) {
        code.addStatement("return $T.privSet()", Empties.class);
      } else if (returnType == ColumnStatistics.class) {
        code.addStatement("return $T.colStats()", Empties.class);
      } else if (returnType == int.class) {
        code.addStatement("return 0");
      } else if (returnType == String[].class) {
        code.addStatement("return new String[0]");
      } else if (returnType == NotificationEventResponse.class) {
        code.addStatement("return $T.event()", Empties.class);
      } else if (returnType == CurrentNotificationEventId.class) {
        code.addStatement("return $T.eventId()", Empties.class);
      } else {
        if (!version.quietReturn.quietReturn(code, returnType)) {
          code.addStatement("return EMPTY");
        }
      }
    } else {
      code.addStatement("return");
      // nothing.
    }
  }

  private interface QuietReturn {
    boolean quietReturn(CodeBlock.Builder code, Class<?> returnType);
  }

  public static enum HiveVersion {
    HIVE2("Hive2", AnnotatedHive2RawStore.class, (a, b) -> false, BaseRawStore.class),
    HIVE3("Hive3", AnnotatedHive3RawStore.class, Hive3EmptyCode::quietReturn, BaseRawStore3.class);

    private final String prefix;
    private final Class<?> annotatedInterface;
    private final QuietReturn quietReturn;
    private final Class<? extends BaseRawStore> baseRawStore;

    HiveVersion(
        String classPrefix,
        Class<?> annotatedInterface,
        QuietReturn quietReturn,
        Class<? extends BaseRawStore> baseRawStore) {
      this.prefix = classPrefix;
      this.annotatedInterface = annotatedInterface;
      this.quietReturn = quietReturn;
      this.baseRawStore = baseRawStore;
    }

    public TypeSpec.Builder nonDelegatingBuilder() {
      return TypeSpec.classBuilder(prefix + "NessieRawStore")
          .superclass(baseRawStore)
          .addModifiers(Modifier.PUBLIC);
    }

    public TypeSpec.Builder delegatingBuilder(TypeSpec nodelegate) {
      return TypeSpec.classBuilder("Delegating" + prefix + "NessieRawStore")
          .superclass(ClassName.bestGuess(nodelegate.name))
          .addModifiers(Modifier.PUBLIC);
    }
  }

  private static class Param {

    private final String name;
    private final Type type;

    public Param(String name, Type type) {
      super();
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public Type getType() {
      return type;
    }
  }

  private static class MethodHolder {
    private final Method method;
    private final boolean extended;

    public MethodHolder(Method method, boolean extended) {
      super();
      this.method = method;
      this.extended = extended;
    }

    boolean extended() {
      return extended;
    }
  }

  public static void main(String... args) {
    int exitCode = new CommandLine(new CodeGenerator()).execute(args);
    System.exit(exitCode);
  }
}
