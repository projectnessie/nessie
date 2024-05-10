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
package org.projectnessie.catalog.formats.iceberg.metrics;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.stream.Stream;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;

@ExtendWith(SoftAssertionsExtension.class)
public class TestMetricsTypes {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  @Disabled("add test cases")
  public void deser(Object nessie, Class<?> nessieType, Class<?> icebergType) throws Exception {}

  static Stream<Arguments> deser() {
    return Stream.of(arguments());
  }

  @ParameterizedTest
  @MethodSource
  public void stringDeser(String json, Class<?> nessieType, Class<?> icebergType) throws Exception {
    IcebergMetricsReport v = IcebergJson.objectMapper().readValue(json, IcebergMetricsReport.class);
    soft.assertThat(v).isInstanceOf(nessieType);

    ObjectMapper mapper = new ObjectMapper();
    RESTSerializers.registerAll(mapper);
    ReportMetricsRequest reportMetricsRequest = mapper.readValue(json, ReportMetricsRequest.class);
    soft.assertThat(reportMetricsRequest.report()).isInstanceOf(icebergType);
  }

  static Stream<Arguments> stringDeser() {
    return Stream.of(
        arguments(
            "{\"report-type\":\"commit-report\",\"table-name\":\"nessie-iceberg-api.newdb.table\",\"snapshot-id\":7199270021382915417,\"sequence-number\":1,\"operation\":\"append\","
                + "\"metrics\":{"
                + "\"total-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":307738775},"
                + "\"attempts\":{\"unit\":\"count\",\"value\":1},"
                + "\"added-data-files\":{\"unit\":\"count\",\"value\":1},"
                + "\"total-data-files\":{\"unit\":\"count\",\"value\":1},"
                + "\"total-delete-files\":{\"unit\":\"count\",\"value\":0},"
                + "\"added-records\":{\"unit\":\"count\",\"value\":2},"
                + "\"total-records\":{\"unit\":\"count\",\"value\":2},"
                + "\"added-files-size-bytes\":{\"unit\":\"bytes\",\"value\":10},"
                + "\"total-files-size-bytes\":{\"unit\":\"bytes\",\"value\":10},"
                + "\"total-positional-deletes\":{\"unit\":\"count\",\"value\":0},"
                + "\"total-equality-deletes\":{\"unit\":\"count\",\"value\":0}"
                + "},"
                + "\"metadata\":{\"iceberg-version\":\"Apache Iceberg 1.4.3 (commit 9a5d24fee239352021a9a73f6a4cad8ecf464f01)\"}}\n",
            IcebergCommitReport.class,
            CommitReport.class));
  }
}
