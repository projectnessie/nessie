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

@ExtendWith({FlociS3Extension.class, SoftAssertionsExtension.class})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ITFlociS3ExtensionStaticField {
  @InjectSoftAssertions private SoftAssertions soft;

  @FlociS3 private static FlociS3Access flociS3StaticField;

  static FlociS3Access memoizedStaticInstance;

  @Order(101)
  @Test
  public void fields1() {
    soft.assertThat(flociS3StaticField).isNotNull();

    soft.assertThat(flociS3StaticField.bucket()).isNotEmpty();
    soft.assertThat(flociS3StaticField.accessKey()).isNotEmpty();
    soft.assertThat(flociS3StaticField.secretKey()).isNotEmpty();

    memoizedStaticInstance = flociS3StaticField;
  }

  @Order(102)
  @Test
  public void fields2() {
    soft.assertThat(flociS3StaticField).isSameAs(memoizedStaticInstance);
  }
}
