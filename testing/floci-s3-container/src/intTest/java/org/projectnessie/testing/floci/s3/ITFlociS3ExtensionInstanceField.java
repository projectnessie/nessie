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
package org.projectnessie.testing.floci.s3;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;

@ExtendWith({FlociS3Extension.class, SoftAssertionsExtension.class})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ITFlociS3ExtensionInstanceField {
  @InjectSoftAssertions private SoftAssertions soft;

  @FlociS3 private FlociS3Access flociS3InstanceField;

  static FlociS3Access memoizedInstanceInstance;

  @Order(101)
  @Test
  public void fields1() {
    soft.assertThat(flociS3InstanceField).isNotNull();

    soft.assertThat(flociS3InstanceField.bucket()).isNotEmpty();
    soft.assertThat(flociS3InstanceField.accessKey()).isNotEmpty();
    soft.assertThat(flociS3InstanceField.secretKey()).isNotEmpty();

    soft.assertThat(flociS3InstanceField.s3endpoint()).isNotEmpty();

    soft.assertThat(flociS3InstanceField.hostPort()).isNotEmpty();

    memoizedInstanceInstance = flociS3InstanceField;
  }

  @Order(102)
  @Test
  public void fields2() {
    soft.assertThat(flociS3InstanceField).isNotNull();

    soft.assertThat(memoizedInstanceInstance).isNotNull().isNotSameAs(flociS3InstanceField);
    soft.assertThat(((GenericContainer<?>) memoizedInstanceInstance).isRunning()).isFalse();

    soft.assertThat(flociS3InstanceField.bucket()).isNotEmpty();
    soft.assertThat(flociS3InstanceField.accessKey()).isNotEmpty();
    soft.assertThat(flociS3InstanceField.secretKey()).isNotEmpty();

    soft.assertThat(flociS3InstanceField).isNotSameAs(memoizedInstanceInstance);

    soft.assertThat(flociS3InstanceField.bucket())
        .isNotEmpty()
        .isNotEqualTo(memoizedInstanceInstance.bucket());

    soft.assertThat(flociS3InstanceField.accessKey())
        .isNotEmpty()
        .isNotEqualTo(memoizedInstanceInstance.accessKey());

    soft.assertThat(flociS3InstanceField.secretKey())
        .isNotEmpty()
        .isNotEqualTo(memoizedInstanceInstance.secretKey());

    soft.assertThat(flociS3InstanceField.s3endpoint())
        .isNotEmpty()
        .isNotEqualTo(memoizedInstanceInstance.s3endpoint());

    soft.assertThat(flociS3InstanceField.hostPort())
        .isNotEmpty()
        .isNotEqualTo(memoizedInstanceInstance.hostPort());
  }
}
