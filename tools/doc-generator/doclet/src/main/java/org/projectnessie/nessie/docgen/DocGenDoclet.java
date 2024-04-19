/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.docgen;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import jdk.javadoc.doclet.Doclet;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Reporter;

public class DocGenDoclet implements Doclet {

  private Path outputDirectory = Paths.get(".");

  private final Option directoryOption =
      new Option() {
        @Override
        public int getArgumentCount() {
          return 1;
        }

        @Override
        public String getDescription() {
          return "Directory to write .md files to";
        }

        @Override
        public Kind getKind() {
          return Kind.STANDARD;
        }

        @Override
        public List<String> getNames() {
          return List.of("-d", "--directory");
        }

        @Override
        public String getParameters() {
          return "directory";
        }

        @Override
        public boolean process(String option, List<String> arguments) {
          outputDirectory = Paths.get(arguments.get(0));
          return true;
        }
      };
  private final Option notimestampDummy = new DummyOption(List.of("-notimestamp"), 0);
  private final Option doctitleDummy = new DummyOption(List.of("-doctitle"), 1);
  private final Option windowtitleDummy = new DummyOption(List.of("-windowtitle"), 1);

  @Override
  public boolean run(DocletEnvironment environment) {
    PropertiesConfigs propertiesConfigs = new PropertiesConfigs(environment);
    SmallryeConfigs smallryeConfigs = new SmallryeConfigs(environment);

    for (Element includedElement : environment.getIncludedElements()) {
      try {
        includedElement.accept(propertiesConfigs.visitor(), null);
        includedElement.accept(smallryeConfigs.visitor(), null);
      } catch (RuntimeException ex) {
        throw new RuntimeException("Failure processing included element " + includedElement, ex);
      }
    }

    for (PropertiesConfigPageGroup page : propertiesConfigs.pages()) {
      System.out.println("Generating properties config pages for " + page.name());
      for (Map.Entry<String, Iterable<PropertiesConfigItem>> e : page.sectionItems().entrySet()) {
        String section = e.getKey();
        if (section.isEmpty()) {
          section = "main";
        }
        System.out.println("... generating page section " + section);
        Iterable<PropertiesConfigItem> items = e.getValue();

        Path file = outputDirectory.resolve(page.name() + "-" + safeFileName(section) + ".md");
        try (BufferedWriter fw = Files.newBufferedWriter(file, UTF_8, CREATE, TRUNCATE_EXISTING);
            PrintWriter writer = new PrintWriter(fw)) {
          writer.println("| Property | Description |");
          writer.println("|----------|-------------|");
          for (PropertiesConfigItem item : items) {
            // TODO add _pluggable_ formatter (javadoc to markdown, later: javadoc to asciidoc?)
            MarkdownPropertyFormatter md = new MarkdownPropertyFormatter(item);
            if (!md.isHidden()) {
              writer.print("| `");
              writer.print(md.propertyName());
              writer.print("` | ");
              writer.print(md.description().replaceAll("\n", "<br>"));
              writer.println(" |");
            }
          }
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }

    for (SmallRyeConfigMappingInfo configMappingInfo : smallryeConfigs.getConfigMappingInfos()) {
      Path file =
          outputDirectory.resolve("smallrye-" + safeFileName(configMappingInfo.prefix) + ".md");
      try (BufferedWriter fw = Files.newBufferedWriter(file, UTF_8, CREATE, TRUNCATE_EXISTING);
          PrintWriter writer = new PrintWriter(fw)) {
        writer.println("| Property | Default Value | Type | Description |");
        writer.println("|----------|---------------|------|-------------|");
        configMappingInfo
            .properties(environment)
            .forEach(
                prop ->
                    writeProperty(
                        prop,
                        writer,
                        configMappingInfo.prefix + '.',
                        smallryeConfigs,
                        environment));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    return true;
  }

  private void writeProperty(
      SmallRyeConfigPropertyInfo prop,
      PrintWriter writer,
      String propertyNamePrefix,
      SmallryeConfigs smallryeConfigs,
      DocletEnvironment environment) {
    MarkdownPropertyFormatter md = new MarkdownPropertyFormatter(prop);
    if (!md.isHidden()) {
      Optional<Class<?>> groupType = prop.groupType();
      String propertyName = md.propertyName();
      String suffix = md.propertySuffix();
      if (!suffix.isEmpty()) {
        propertyName += ".`_`<" + suffix + ">`_`";
      }
      String fullName = propertyNamePrefix + propertyName;

      writer.print("| ");
      String fullNameCode = ('`' + fullName + '`').replaceAll("``", "");
      writer.print(fullNameCode);
      writer.print(" | ");
      String dv = prop.defaultValue();
      if (dv != null) {
        if (dv.isEmpty()) {
          writer.print("(empty)");
        } else {
          writer.print('`');
          writer.print(dv);
          writer.print('`');
        }
      }
      writer.print(" | ");
      writer.print(md.propertyType());
      writer.print(" | ");
      writer.print(md.description().replaceAll("\n", "<br>"));
      writer.println(" |");

      if (groupType.isPresent()) {
        String pre = fullName + '.';
        smallryeConfigs
            .getConfigMappingInfo(groupType.get())
            .properties(environment)
            .forEach(p -> writeProperty(p, writer, pre, smallryeConfigs, environment));
      }
    }
  }

  private String safeFileName(String str) {
    StringBuilder sb = new StringBuilder();
    int len = str.length();
    boolean hadLOD = false;
    for (int i = 0; i < len; i++) {
      char c = str.charAt(i);
      if (Character.isLetterOrDigit(c)) {
        sb.append(c);
        hadLOD = true;
      } else {
        if (hadLOD) {
          sb.append('_');
          hadLOD = false;
        }
      }
    }
    return sb.toString();
  }

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.latest();
  }

  @Override
  public Set<? extends Option> getSupportedOptions() {
    return Set.of(directoryOption, doctitleDummy, windowtitleDummy, notimestampDummy);
  }

  @Override
  public String getName() {
    return "NessieDoclet";
  }

  @Override
  public void init(Locale locale, Reporter reporter) {}

  static final class DummyOption implements Option {
    private final List<String> names;
    private final int argumentCount;

    DummyOption(List<String> names, int argumentCount) {
      this.names = names;
      this.argumentCount = argumentCount;
    }

    @Override
    public boolean process(String option, List<String> arguments) {
      return true;
    }

    @Override
    public String getParameters() {
      return "";
    }

    @Override
    public List<String> getNames() {
      return names;
    }

    @Override
    public Kind getKind() {
      return null;
    }

    @Override
    public String getDescription() {
      return "Ignored";
    }

    @Override
    public int getArgumentCount() {
      return argumentCount;
    }
  }
}
