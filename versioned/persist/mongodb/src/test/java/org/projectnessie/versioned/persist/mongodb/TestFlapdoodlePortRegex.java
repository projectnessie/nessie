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
package org.projectnessie.versioned.persist.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import de.flapdoodle.embed.process.io.StreamProcessor;
import java.util.regex.Matcher;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.persist.mongodb.FlapdoodleMongoTestConnectionProviderSource.PortExtractor;

public class TestFlapdoodlePortRegex {
  @ParameterizedTest
  @ValueSource(
      strings = {
        // Flapdoodle-Mongo 3.1.4
        "NETWORK  [initandlisten] waiting for connections on port 35763\n",
        "NETWORK  [initandlisten] some stuff before\nNETWORK  [initandlisten] waiting for connections on port 35763\n",
        "NETWORK  [initandlisten] waiting for connections on port 1000\n",
        // Some longer snippet
        "2021-12-09T14:56:55.240+0100 I STORAGE  [main] Max cache overflow file size custom option: 0\n"
            + "2021-12-09T14:56:55.240+0100 I CONTROL  [main] note: noprealloc may hurt performance in many applications\n"
            + "2021-12-09T14:56:55.244+0100 I CONTROL  [initandlisten] MongoDB starting : pid=439588 port=0 dbpath=/tmp/embedmongo-db-55b85226-2e1b-4529-9c0a-9168b69aaae7 64-bit host=bear\n"
            + "2021-12-09T14:56:55.244+0100 I CONTROL  [initandlisten] db version v4.0.12\n"
            + "2021-12-09T14:56:55.244+0100 I CONTROL  [initandlisten] git version: 5776e3cbf9e7afe86e6b29e22520ffb6766e95d4\n"
            + "2021-12-09T14:56:55.244+0100 I CONTROL  [initandlisten] allocator: tcmalloc\n"
            + "2021-12-09T14:56:55.244+0100 I CONTROL  [initandlisten] modules: none\n"
            + "2021-12-09T14:56:55.244+0100 I CONTROL  [initandlisten] build environment:\n"
            + "2021-12-09T14:56:55.244+0100 I CONTROL  [initandlisten]     distarch: x86_64\n"
            + "2021-12-09T14:56:55.244+0100 I CONTROL  [initandlisten]     target_arch: x86_64\n"
            + "2021-12-09T14:56:55.244+0100 I CONTROL  [initandlisten] options: { net: { port: 0 }, security: { authorization: \"disabled\" }, storage: { dbPath: \"/tmp/embedmongo-db-55b85226-2e1b-4529-9c0a-9168b69aaae7\", journal: { enabled: false }, mmapv1: { preallocDataFiles: false, smallFiles: true }, syncPeriodSecs: 0.0 } }\n"
            + "2021-12-09T14:56:55.244+0100 I STORAGE  [initandlisten] wiredtiger_open config: create,cache_size=31595M,cache_overflow=(file_max=0M),session_max=20000,eviction=(threads_min=4,threads_max=4),config_base=false,statistics=(fast),log=(enabled=true,archive=true,path=journal,compressor=snappy),file_manager=(close_idle_time=100000),statistics_log=(wait=0),verbose=(recovery_progress),,log=(enabled=false),\n"
            + "2021-12-09T14:56:55.792+0100 I STORAGE  [initandlisten] WiredTiger message [1639058215:792247][439588:0x7fae47f1fa40], txn-recover: Set global recovery timestamp: 0\n"
            + "2021-12-09T14:56:55.793+0100 I RECOVERY [initandlisten] WiredTiger recoveryTimestamp. Ts: Timestamp(0, 0)\n"
            + "2021-12-09T14:56:55.796+0100 W STORAGE  [initandlisten] Detected configuration for non-active storage engine mmapv1 when current storage engine is wiredTiger\n"
            + "2021-12-09T14:56:55.796+0100 I CONTROL  [initandlisten] \n"
            + "2021-12-09T14:56:55.796+0100 I CONTROL  [initandlisten] ** WARNING: This server is bound to localhost.\n"
            + "2021-12-09T14:56:55.796+0100 I CONTROL  [initandlisten] **          Remote systems will be unable to connect to this server. \n"
            + "2021-12-09T14:56:55.796+0100 I CONTROL  [initandlisten] **          Start the server with --bind_ip <address> to specify which IP \n"
            + "2021-12-09T14:56:55.796+0100 I CONTROL  [initandlisten] **          addresses it should serve responses from, or with --bind_ip_all to\n"
            + "2021-12-09T14:56:55.796+0100 I CONTROL  [initandlisten] **          bind to all interfaces. If this behavior is desired, start the\n"
            + "2021-12-09T14:56:55.796+0100 I CONTROL  [initandlisten] **          server with --bind_ip 127.0.0.1 to disable this warning.\n"
            + "2021-12-09T14:56:55.796+0100 I CONTROL  [initandlisten] \n"
            + "2021-12-09T14:56:55.796+0100 I CONTROL  [initandlisten] \n"
            + "2021-12-09T14:56:55.796+0100 I CONTROL  [initandlisten] ** WARNING: soft rlimits too low. rlimits set to 32768 processes, 100000 files. Number of processes should be at least 50000 : 0.5 times number of files.\n"
            + "2021-12-09T14:56:55.796+0100 I STORAGE  [initandlisten] createCollection: admin.system.version with provided UUID: b6a8a0d3-ab93-4dc8-a269-34522a1cdfe4\n"
            + "2021-12-09T14:56:55.798+0100 I COMMAND  [initandlisten] setting featureCompatibilityVersion to 4.0\n"
            + "2021-12-09T14:56:55.802+0100 I STORAGE  [initandlisten] createCollection: local.startup_log with generated UUID: 058cd03a-8096-489b-ba38-ece78b55ccd1\n"
            + "2021-12-09T14:56:55.805+0100 I FTDC     [initandlisten] Initializing full-time diagnostic data capture with directory '/tmp/embedmongo-db-55b85226-2e1b-4529-9c0a-9168b69aaae7/diagnostic.data'\n"
            + "2021-12-09T14:56:55.806+0100 I NETWORK  [initandlisten] waiting for connections on port 42899\n",

        // Flapdoodle-Mongo 3.2.2
        "\"c\":\"NETWORK\",  \"id\":23016,   \"ctx\":\"listener\",\"msg\":\"Waiting for connections\",\"attr\":{\"port\":43759,\"ssl\":\"off\"}}",
        "\"c\":\"NETWORK\",  \"id\":23016,   \"ctx\":\"listener\",\"msg\":\"Waiting for connections\",\"attr\":{\"port\":43759,\"ssl\":\"off\"}}\n",
      })
  public void portRegexPositive(String logLines) {
    Matcher matcher =
        FlapdoodleMongoTestConnectionProviderSource.LISTEN_ON_PORT_PATTERN.matcher(logLines);
    assertThat(matcher)
        .satisfies(m -> assertThat(m.matches()).isTrue())
        .extracting(m -> m.group(1))
        .extracting(Integer::parseInt)
        .asInstanceOf(InstanceOfAssertFactories.INTEGER)
        .isGreaterThanOrEqualTo(1000);

    // Validate PortExtractor implementation with the full logLines
    PortExtractor portExtractor = newPortExtractor();
    portExtractor.process(logLines);
    assertThat(portExtractor.getPort()).isGreaterThan(0);

    // Validate PortExtractor implementation with splits of the logLines
    for (int splitSize = 1; splitSize < 20; splitSize++) {
      portExtractor = newPortExtractor();
      for (int offset = 0; offset < logLines.length(); offset += splitSize) {
        int endIndex = Math.min(offset + splitSize, logLines.length());
        String block =
            endIndex == logLines.length()
                ? logLines.substring(offset)
                : logLines.substring(offset, endIndex);
        portExtractor.process(block);
      }
      assertThat(portExtractor.getPort())
          .describedAs("With split-size={}", splitSize)
          .isGreaterThan(0);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        // Should not match, single-digit port number
        "NETWORK  [initandlisten] waiting for connections on port 0",
        "NETWORK  [initandlisten] waiting for connections on port 0\n",
        // Some other message
        "NETWORK  [listener] connection accepted from 127.0.0.1:38074 #2 (2 connections now open)",

        // Flapdoodle-Mongo 3.2.2
        "\"c\":\"NETWORK\",  \"id\":23015,   \"ctx\":\"listener\",\"msg\":\"Listening on\",\"attr\":{\"address\":\"127.0.0.1\"}}",
        "\"c\":\"NETWORK\",  \"id\":22943,   \"ctx\":\"listener\",\"msg\":\"Connection accepted\",\"attr\":{\"remote\":\"127.0.0.1:49808\",\"connectionId\":1,\"connectionCount\":1}}"
      })
  public void portRegexNegative(String logLines) {
    Matcher matcher =
        FlapdoodleMongoTestConnectionProviderSource.LISTEN_ON_PORT_PATTERN.matcher(logLines);
    assertThat(matcher)
        .extracting(Matcher::matches)
        .asInstanceOf(InstanceOfAssertFactories.BOOLEAN)
        .isFalse();

    // Validate PortExtractor implementation with full logLines
    PortExtractor portExtractor = newPortExtractor();
    portExtractor.process(logLines);
    assertThat(portExtractor.getPort()).isEqualTo(0);

    // Validate PortExtractor implementation with splits of the logLines
    for (int splitSize = 1; splitSize < 20; splitSize++) {
      portExtractor = newPortExtractor();
      for (int offset = 0; offset < logLines.length(); offset += splitSize) {
        int endIndex = Math.min(offset + splitSize, logLines.length());
        String block =
            endIndex == logLines.length()
                ? logLines.substring(offset)
                : logLines.substring(offset, endIndex);
        portExtractor.process(block);
      }
      assertThat(portExtractor.getPort()).describedAs("With split-size={}", splitSize).isEqualTo(0);
    }
  }

  private static PortExtractor newPortExtractor() {
    return new PortExtractor(
        new StreamProcessor() {
          @Override
          public void process(String s) {}

          @Override
          public void onProcessed() {}
        });
  }
}
