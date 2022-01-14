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
package org.projectnessie.test.compatibility;

import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.projectnessie.test.compatibility.Util.FIELD_NESSIE_VERSION;

import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.TestInstantiationException;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ClassBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClassBuilder.class);

  private ClassBuilder() {}

  static Collection<Class<?>> createTestClasses(
      String nessieVersion, String versionInClass, String classNameSuffix, URL[] classpathUrls) {
    try {
      LOGGER.debug("Creating dynamic test classes for Nessie version '{}'", nessieVersion);

      Map<String, byte[]> generatedClasses = new HashMap<>();

      generateRestTest(classNameSuffix, versionInClass, generatedClasses);
      generateResteasyTest(classNameSuffix, versionInClass, generatedClasses);

      ClassLoader cl =
          new URLClassLoader(classpathUrls, Thread.currentThread().getContextClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
              byte[] classBytes = generatedClasses.get(name);
              if (classBytes != null) {
                Class<?> clazz = defineClass(name, classBytes, 0, classBytes.length);
                if (resolve) {
                  resolveClass(clazz);
                }
                return clazz;
              }
              return super.loadClass(name, resolve);
            }
          };

      return generatedClasses.keySet().stream()
          .map(
              bytes -> {
                try {
                  LOGGER.debug("Loading dynamic test class '{}'", bytes);
                  return cl.loadClass(bytes);
                } catch (ClassNotFoundException ex) {
                  throw new RuntimeException(ex);
                }
              })
          .collect(Collectors.toList());
    } catch (Exception e) {
      LOGGER.error("Failed to construct test class for Nessie version '{}'", nessieVersion, e);
      throw new TestInstantiationException("Failed to construct test class", e);
    }
  }

  private static void generateRestTest(
      String classNameSuffix, String versionInClass, Map<String, byte[]> generatedClasses) {
    String className =
        String.format(
            "%s.nessieclientcompat.ITTestRest_%s",
            ClassBuilder.class.getPackage().getName(), classNameSuffix);
    Type classType = Type.getType(typeName(className));
    Type typeRestTestBase = Type.getType(typeName(Util.CLS_NAME_ABSTRACT_TEST_REST));
    Type uriType = Type.getType(URI.class);

    ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    classWriter.visit(
        Opcodes.V11,
        ACC_PUBLIC | ACC_SUPER,
        slashify(className),
        null,
        slashify(Util.CLS_NAME_ABSTRACT_TEST_REST),
        null);

    classWriter.visitField(
        ACC_PUBLIC | ACC_STATIC, "testUri", typeName(URI.class.getName()), null, null);

    classWriter.visitField(
        ACC_PUBLIC | ACC_STATIC,
        FIELD_NESSIE_VERSION,
        typeName(String.class.getName()),
        null,
        versionInClass);

    // Following produces the default constructor like this:
    /*
    public <init>() {
      testUri = URI.create(uriString);
    }
     */
    Method methodInit = Method.getMethod("void <init> ()");
    GeneratorAdapter methodGenerator =
        new GeneratorAdapter(ACC_PUBLIC, methodInit, null, null, classWriter);
    methodGenerator.loadThis();
    methodGenerator.invokeConstructor(typeRestTestBase, methodInit);
    //
    methodGenerator.returnValue();
    methodGenerator.endMethod();

    Method methodTestServerUri =
        Method.getMethod(
            String.format("void %s (java.net.URI)", Util.METHOD_UPDATE_TEST_SERVER_URI));
    methodGenerator =
        new GeneratorAdapter(ACC_PUBLIC | ACC_STATIC, methodTestServerUri, null, null, classWriter);
    //
    methodGenerator.loadArg(0);
    methodGenerator.putStatic(classType, "testUri", uriType);
    //
    methodGenerator.returnValue();
    methodGenerator.endMethod();

    /*
    Following produces:
      public void setUp() {
        init(URI.create(uriString));   // uses the 'uriString' argument
      }
     */
    methodGenerator =
        new GeneratorAdapter(ACC_PUBLIC, Method.getMethod("void setUp()"), null, null, classWriter);
    methodGenerator.visitAnnotation(Type.getType(BeforeEach.class).getDescriptor(), true);
    methodGenerator.loadThis();
    methodGenerator.getStatic(classType, "testUri", uriType);
    methodGenerator.invokeVirtual(
        Type.getType(typeName(Util.CLS_NAME_ABSTRACT_TEST_REST)),
        Method.getMethod(String.format("void init(%s)", URI.class.getName())));
    methodGenerator.returnValue();
    methodGenerator.endMethod();

    classWriter.visitEnd();

    generatedClasses.put(className, classWriter.toByteArray());
  }

  private static void generateResteasyTest(
      String classNameSuffix, String versionInClass, Map<String, byte[]> generatedClasses) {
    String className =
        String.format(
            "%s.nessieclientcompat.ITTestResteasy_%s",
            ClassBuilder.class.getPackage().getName(), classNameSuffix);
    Type classType = Type.getType(typeName(className));
    Type stringType = Type.getType(String.class);
    Type uriType = Type.getType(URI.class);
    Type typeResteasyTestBase = Type.getType(typeName(Util.CLS_NAME_ABSTRACT_TEST_RESTEASY));
    Type restAssuredType = Type.getType(typeName(Util.CLS_NAME_REST_ASSURED));

    ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    classWriter.visit(
        Opcodes.V11,
        ACC_PUBLIC | ACC_SUPER,
        slashify(className),
        null,
        slashify(Util.CLS_NAME_ABSTRACT_TEST_RESTEASY),
        null);

    classWriter.visitField(
        ACC_PUBLIC | ACC_STATIC, "testUri", typeName(URI.class.getName()), null, null);

    classWriter.visitField(
        ACC_PUBLIC | ACC_STATIC,
        FIELD_NESSIE_VERSION,
        typeName(String.class.getName()),
        null,
        versionInClass);

    // Following produces the default constructor.
    Method methodInit = Method.getMethod("void <init> ()");
    GeneratorAdapter methodGenerator =
        new GeneratorAdapter(ACC_PUBLIC, methodInit, null, null, classWriter);
    methodGenerator.loadThis();
    methodGenerator.invokeConstructor(typeResteasyTestBase, methodInit);
    //
    methodGenerator.returnValue();
    methodGenerator.endMethod();

    Method methodTestServerUri =
        Method.getMethod(
            String.format("void %s (java.net.URI)", Util.METHOD_UPDATE_TEST_SERVER_URI));
    methodGenerator =
        new GeneratorAdapter(ACC_PUBLIC | ACC_STATIC, methodTestServerUri, null, null, classWriter);
    //
    methodGenerator.loadArg(0);
    methodGenerator.putStatic(classType, "testUri", uriType);
    //
    methodGenerator.loadArg(0);
    methodGenerator.invokeVirtual(uriType, Method.getMethod("java.lang.String getPath ()"));
    methodGenerator.putStatic(typeResteasyTestBase, "basePath", stringType);
    //
    methodGenerator.loadArg(0);
    methodGenerator.invokeVirtual(uriType, Method.getMethod("java.lang.String getHost ()"));
    methodGenerator.storeLocal(1, stringType);
    //
    methodGenerator.loadArg(0);
    methodGenerator.invokeVirtual(uriType, Method.getMethod("java.lang.String getScheme ()"));
    methodGenerator.push("://");
    methodGenerator.invokeVirtual(
        stringType, Method.getMethod("java.lang.String concat (java.lang.String)="));
    methodGenerator.loadLocal(1, stringType);
    methodGenerator.invokeVirtual(
        stringType, Method.getMethod("java.lang.String concat (java.lang.String)="));
    methodGenerator.putStatic(restAssuredType, "baseURI", stringType);
    //
    methodGenerator.loadArg(0);
    methodGenerator.invokeVirtual(uriType, Method.getMethod("int getPort ()"));
    methodGenerator.putStatic(restAssuredType, "port", Type.getType(int.class));
    //
    methodGenerator.loadArg(0);
    methodGenerator.invokeVirtual(uriType, Method.getMethod("java.lang.String getPath ()"));
    methodGenerator.putStatic(restAssuredType, "basePath", stringType);
    //
    methodGenerator.returnValue();
    methodGenerator.endMethod();

    classWriter.visitEnd();

    generatedClasses.put(className, classWriter.toByteArray());
  }

  private static String slashify(String cls) {
    return cls.replace('.', '/');
  }

  private static String typeName(String cls) {
    return String.format("L%s;", cls.replace('.', '/'));
  }
}
