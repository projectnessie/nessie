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
import static org.projectnessie.nessie.docgen.SmallRyeConfigs.concatWithDot;

import com.sun.source.doctree.DocCommentTree;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
    SmallRyeConfigs smallryeConfigs = new SmallRyeConfigs(environment);

    for (Element includedElement : environment.getIncludedElements()) {
      try {
        includedElement.accept(propertiesConfigs.visitor(), null);
        includedElement.accept(smallryeConfigs.visitor(), null);
      } catch (RuntimeException ex) {
        throw new RuntimeException("Failure processing included element " + includedElement, ex);
      }
    }

    propertiesConfigPages(propertiesConfigs);

    smallryeConfigPages(environment, smallryeConfigs);

    return true;
  }

  private void propertiesConfigPages(PropertiesConfigs propertiesConfigs) {
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
  }

  private void smallryeConfigPages(DocletEnvironment environment, SmallRyeConfigs smallryeConfigs) {
    Map<String, SmallRyeConfigSectionPage> sectionPages = new HashMap<>();

    for (SmallRyeConfigMappingInfo mappingInfo : smallryeConfigs.configMappingInfos()) {
      smallryeProcessRootMappingInfo(environment, smallryeConfigs, mappingInfo, sectionPages);
    }

    sectionPages.values().stream()
        .filter(p -> !p.isEmpty())
        .forEach(
            page -> {
              System.out.printf(
                  "... generating smallrye config page for section %s%n", page.section);
              Path file = outputDirectory.resolve("smallrye-" + safeFileName(page.section) + ".md");
              try (PrintWriter pw =
                  new PrintWriter(
                      Files.newBufferedWriter(file, UTF_8, CREATE, TRUNCATE_EXISTING))) {
                page.writeTo(pw);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private void smallryeProcessRootMappingInfo(
      DocletEnvironment environment,
      SmallRyeConfigs smallryeConfigs,
      SmallRyeConfigMappingInfo mappingInfo,
      Map<String, SmallRyeConfigSectionPage> sectionPages) {
    String effectiveSection = mappingInfo.prefix();
    String propertyNamePrefix = mappingInfo.prefix();
    smallryeProcessMappingInfo(
        "",
        environment,
        smallryeConfigs,
        effectiveSection,
        mappingInfo,
        propertyNamePrefix,
        sectionPages);
  }

  private void smallryeProcessPropertyMappingInfo(
      String logIndent,
      DocletEnvironment environment,
      SmallRyeConfigs smallryeConfigs,
      String section,
      SmallRyeConfigMappingInfo mappingInfo,
      String propertyNamePrefix,
      Map<String, SmallRyeConfigSectionPage> sectionPages) {
    smallryeProcessMappingInfo(
        logIndent + "  ",
        environment,
        smallryeConfigs,
        section,
        mappingInfo,
        propertyNamePrefix,
        sectionPages);
  }

  private void smallryeProcessMappingInfo(
      String logIndent,
      DocletEnvironment environment,
      SmallRyeConfigs smallryeConfigs,
      String effectiveSection,
      SmallRyeConfigMappingInfo mappingInfo,
      String propertyNamePrefix,
      Map<String, SmallRyeConfigSectionPage> sectionPages) {

    // Eagerly create page, so we have the comment from the type.
    sectionPages.computeIfAbsent(
        effectiveSection,
        s -> new SmallRyeConfigSectionPage(s, mappingInfo.element(), mappingInfo.typeComment()));

    mappingInfo
        .properties(environment)
        .forEach(
            prop ->
                smallryeProcessProperty(
                    logIndent,
                    environment,
                    smallryeConfigs,
                    mappingInfo,
                    effectiveSection,
                    prop,
                    propertyNamePrefix,
                    sectionPages));
  }

  private void smallryeProcessProperty(
      String logIndent,
      DocletEnvironment environment,
      SmallRyeConfigs smallryeConfigs,
      SmallRyeConfigMappingInfo mappingInfo,
      String section,
      SmallRyeConfigPropertyInfo propertyInfo,
      String propertyNamePrefix,
      Map<String, SmallRyeConfigSectionPage> sectionPages) {

    String effectiveSection =
        propertyInfo.prefixOverride().map(o -> concatWithDot(section, o)).orElse(section);

    MarkdownPropertyFormatter md = new MarkdownPropertyFormatter(propertyInfo);
    if (md.isHidden()) {
      return;
    }
    String fullName =
        formatPropertyName(propertyNamePrefix, md.propertyName(), md.propertySuffix());

    SmallRyeConfigSectionPage page =
        sectionPages.computeIfAbsent(
            effectiveSection,
            s -> {
              DocCommentTree doc =
                  propertyInfo.sectionDocFromType()
                      ? propertyInfo
                          .groupType()
                          .map(smallryeConfigs::getConfigMappingInfo)
                          .map(SmallRyeConfigMappingInfo::typeComment)
                          .orElse(null)
                      : propertyInfo.doc();
              return new SmallRyeConfigSectionPage(s, mappingInfo.element(), doc);
            });
    propertyInfo.prefixOverride().ifPresent(o -> page.incrementSectionRef());
    if (propertyInfo.isSettableType()) {
      page.addProperty(fullName, propertyInfo, md);
    }

    propertyInfo
        .groupType()
        .ifPresent(
            groupType ->
                smallryeProcessPropertyMappingInfo(
                    logIndent + "  ",
                    environment,
                    smallryeConfigs,
                    effectiveSection,
                    smallryeConfigs.getConfigMappingInfo(groupType),
                    fullName,
                    sectionPages));
  }

  private String formatPropertyName(
      String propertyNamePrefix, String propertyName, String propertySuffix) {
    String r = concatWithDot(propertyNamePrefix, propertyName);
    return propertySuffix.isEmpty() ? r : concatWithDot(r, "`_`<" + propertySuffix + ">`_`");
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
