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

/**
 * Main Nessie APIs.
 */
@OpenAPIDefinition(
    info = @Info(
        title = "Nessie API",
        version = "0.6.0",
        contact = @Contact(
            name = "Project Nessie",
            url = "https://projectnessie.org"),
        license = @License(
            name = "Apache 2.0",
            url = "http://www.apache.org/licenses/LICENSE-2.0.html")),
    components = @Components(examples = {@ExampleObject(name = "ContentsKey", value = "{\n"
        + "      \"elements\": [\n"
        + "        \"example\"\n"
        + "        \"key\"\n"
        + "      ]\n"
        + "    }"),
      @ExampleObject(name = "iceberg", value = "{\n"
        + "    \"type\": \"ICEBERG_TABLE\",\n"
        + "    \"metadataLocation\": \"/path/to/metadata/\",\n"
        + "    \"uuid\": \"b874b5d5-f926-4eed-9be7-b2380d9810c0\"\n"
        + "}"),
      @ExampleObject(name = "ref", value = "main"),
      @ExampleObject(name = "javaInstant", value = "2021-05-31T08:23:15Z"),
      @ExampleObject(name = "hash", value = "abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122"),
      @ExampleObject(name = "commitMessage", value = "testCommitMessage"),
      @ExampleObject(name = "multiGetResponse", value = "{\n"
        + "  \"contents\": [\n"
        + "    {\n"
        + "      \"contents\": {\n"
        + "        \"type\": \"ICEBERG_TABLE\",\n"
        + "        \"uuid\": \"b874b5d5-f926-4eed-9be7-b2380d9810c0\",\n"
        + "        \"metadataLocation\": \"/path/to/metadata/\"\n"
        + "      },\n"
        + "      \"key\": {\n"
        + "        \"elements\": [\n"
        + "          \"example\"\n"
        + "          \"key\"\n"
        + "        ]\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}"),
      @ExampleObject(name = "multiGetRequest", value = "{\n"
        + "  \"requestedKeys\": [\n"
        + "    {\n"
        + "      \"elements\": [\n"
        + "        \"example\"\n"
        + "        \"key\"\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}"),
      @ExampleObject(name = "refObj", value = "{\n"
        + "  \"type\": \"BRANCH\""
        + "  \"hash\": \"abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122\",\n"
        + "  \"name\": \"main\"\n"
        + "}"),
      @ExampleObject(name = "tagObj", value = "{\n"
        + "  \"type\": \"TAG\""
        + "  \"hash\": \"abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122\",\n"
        + "  \"name\": \"exampleTag\"\n"
        + "}"),
      @ExampleObject(name = "entriesResponse", value = "{\n"
        + "  \"token\": \"xxx\",\n"
        + "  \"entries\": [\n"
        + "    {\n"
        + "      \"name\": {\n"
        + "        \"elements\": [\n"
        + "          \"example\"\n"
        + "          \"key\"\n"
        + "        ]\n"
        + "      },\n"
        + "      \"type\": \"ICEBERG_TABLE\"\n"
        + "    }\n"
        + "  ]\n"
        + "}"),
      @ExampleObject(name = "types", value = "[\"ICEBERG_TABLE\"]"),
      @ExampleObject(name = "merge", value = "{\n"
        + "  \"fromHash\": \"abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122\"\n"
        + "}"),
      @ExampleObject(name = "transplant", value = "{\n"
        + "  \"hashesToTransplant\": [\n"
        + "    \"abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122\"\n"
        + "  ]\n"
        + "}"),
      @ExampleObject(name = "operations", value = "{\n"
        + "  \"commitMeta\": {\n"
        + "    \"author\": \"authorName <authorName@example.com>\",\n"
        + "    \"authorTime\": \"2021-04-07T14:42:25.534748Z\",\n"
        + "    \"commitTime\": \"2021-04-07T14:42:25.534748Z\",\n"
        + "    \"committer\": \"committerName <committerName@example.com>\",\n"
        + "    \"hash\": \"abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122\",\n"
        + "    \"message\": \"testCommitMessage\",\n"
        + "    \"properties\": {\n"
        + "      \"additionalProp1\": \"xxx\",\n"
        + "      \"additionalProp2\": \"yyy\",\n"
        + "      \"additionalProp3\": \"zzz\"\n"
        + "    },\n"
        + "    \"signedOffBy\": \"signedOffByName <signedOffBy@example.com>\"\n"
        + "  },\n"
        + "  \"operations\": [\n"
        + "    {\n"
        + "      \"key\": {\n"
        + "        \"elements\": [\n"
        + "          \"example\"\n"
        + "          \"key\"\n"
        + "        ]\n"
        + "      },\n"
        + "      \"contents\": {\n"
        + "        \"type\": \"ICEBERG_TABLE\"\n"
        + "        \"uuid\": \"b874b5d5-f926-4eed-9be7-b2380d9810c0\",\n"
        + "        \"metadataLocation\": \"/path/to/metadata/\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}"),
      @ExampleObject(name = "logResponse", value = "{\n"
        + "  \"token\": \"xxx\",\n"
        + "  \"operations\": [\n"
        + "    {\n"
        + "      \"author\": \"authorName <authorName@example.com>\",\n"
        + "      \"authorTime\": \"2021-04-07T14:42:25.534748Z\",\n"
        + "      \"commitTime\": \"2021-04-07T14:42:25.534748Z\",\n"
        + "      \"committer\": \"committerName <committerName@example.com>\",\n"
        + "      \"hash\": \"abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122\",\n"
        + "      \"message\": \"testCommitMessage\",\n"
        + "      \"properties\": {\n"
        + "        \"additionalProp1\": \"xxx\",\n"
        + "        \"additionalProp2\": \"yyy\",\n"
        + "        \"additionalProp3\": \"zzz\"\n"
        + "      },\n"
        + "      \"signedOffBy\": \"signedOffByName <signedOffBy@example.com>\"\n"
        + "    }\n"
        + "  ]\n"
        + "}")})
)
package org.projectnessie.api;

import org.eclipse.microprofile.openapi.annotations.Components;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.info.Contact;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.eclipse.microprofile.openapi.annotations.info.License;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
