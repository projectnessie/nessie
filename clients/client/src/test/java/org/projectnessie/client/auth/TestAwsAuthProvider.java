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
package org.projectnessie.client.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.RequestFilter;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.regions.Region;

class TestAwsAuthProvider {

  @AfterEach
  void cleanupSystemProperties() {
    System.getProperties().remove(SdkSystemSetting.AWS_ACCESS_KEY_ID.property());
    System.getProperties().remove(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property());
    System.getProperties().remove(ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.property());
  }

  @Test
  void testAws() {
    Map<String, String> authCfg =
        ImmutableMap.of(
            NessieConfigConstants.CONF_NESSIE_AUTH_TYPE,
            AwsAuthenticationProvider.AUTH_TYPE_VALUE,
            NessieConfigConstants.CONF_NESSIE_AWS_REGION,
            "eu-central-1");
    assertThat(NessieAuthenticationProvider.fromConfig(authCfg::get))
        .isNotNull()
        .extracting(Object::getClass)
        .extracting(Class::getSimpleName)
        .isEqualTo("AwsAuthentication");

    authCfg =
        ImmutableMap.of(
            NessieConfigConstants.CONF_NESSIE_AUTH_TYPE, AwsAuthenticationProvider.AUTH_TYPE_VALUE);
    assertThat(NessieAuthenticationProvider.fromConfig(authCfg::get))
        .isNotNull()
        .extracting(Object::getClass)
        .extracting(Class::getSimpleName)
        .isEqualTo("AwsAuthentication");

    ImmutableMap<String, String> authCfgErr =
        ImmutableMap.of(
            NessieConfigConstants.CONF_NESSIE_AUTH_TYPE,
            AwsAuthenticationProvider.AUTH_TYPE_VALUE,
            NessieConfigConstants.CONF_NESSIE_AWS_REGION,
            "not-on-this-planet-1");
    assertThatThrownBy(() -> NessieAuthenticationProvider.fromConfig(authCfgErr::get))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown region 'not-on-this-planet-1'.");
  }

  @Test
  void testFromConfig() {
    Map<String, String> authCfg =
        ImmutableMap.of(
            NessieConfigConstants.CONF_NESSIE_AUTH_TYPE, AwsAuthenticationProvider.AUTH_TYPE_VALUE);

    NessieAuthentication authentication = NessieAuthenticationProvider.fromConfig(authCfg::get);

    System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), "testFromConfig");
    System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), "test_secret");

    checkAuth(authentication, Region.US_WEST_2.toString(), "testFromConfig");
  }

  @Test
  void testStaticBuilder() {
    System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), "testStaticBuilder");
    System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), "test_secret");

    checkAuth(
        AwsAuthenticationProvider.create(Region.AWS_GLOBAL),
        Region.AWS_GLOBAL.toString(),
        "testStaticBuilder");
  }

  @Test
  void testFromConfigWithProfile(@TempDir File configDir) throws IOException {
    Map<String, String> authCfg =
        ImmutableMap.<String, String>builder()
            .put(
                NessieConfigConstants.CONF_NESSIE_AUTH_TYPE,
                AwsAuthenticationProvider.AUTH_TYPE_VALUE)
            .put(NessieConfigConstants.CONF_NESSIE_AWS_REGION, Region.US_EAST_1.toString())
            .put(NessieConfigConstants.CONF_NESSIE_AWS_PROFILE, "test2")
            .build();

    NessieAuthentication authentication = NessieAuthenticationProvider.fromConfig(authCfg::get);

    writeAwsProfile(configDir, "test2", "testFromConfigWithProfile");

    checkAuth(authentication, Region.US_EAST_1.toString(), "testFromConfigWithProfile");
  }

  @Test
  void testStaticBuilderWithProfile(@TempDir File configDir) throws IOException {
    writeAwsProfile(configDir, "test1", "testStaticBuilderWithProfile");

    checkAuth(
        AwsAuthenticationProvider.create(Region.AWS_GLOBAL, "test1"),
        Region.AWS_GLOBAL.toString(),
        "testStaticBuilderWithProfile");
  }

  void writeAwsProfile(File configDir, String profileName, String keyId) throws IOException {
    File credentials = new File(configDir, "aws.credentials");
    Files.write(
        credentials.toPath(),
        ImmutableList.of(
            "[" + profileName + "]",
            "aws_access_key_id=" + keyId,
            "aws_secret_access_key=test1_secret",
            "aws_session_token=test1_session"),
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE);

    System.setProperty(
        ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.property(),
        credentials.getAbsolutePath());
  }

  void checkAuth(NessieAuthentication authentication, String expectedRegion, String expectedKeyId) {
    assertThat(authentication).isInstanceOf(HttpAuthentication.class);
    HttpAuthentication httpAuthentication = (HttpAuthentication) authentication;

    // Intercept the call to HttpClient.register(RequestFilter) and extract the RequestFilter for
    // our test
    RequestFilter[] authFilter = new RequestFilter[1];
    HttpClient client = Mockito.mock(HttpClient.class);
    Mockito.doAnswer(
            invocationOnMock -> {
              Object[] args = invocationOnMock.getArguments();
              if (args.length == 1
                  && args[0] instanceof RequestFilter
                  && args[0]
                      .getClass()
                      .getSimpleName()
                      .equalsIgnoreCase("AwsHttpAuthenticationFilter")) {
                authFilter[0] = (RequestFilter) args[0];
              }
              return null;
            })
        .when(client)
        .register((RequestFilter) Mockito.any());
    httpAuthentication.applyToHttpClient(client);

    // Check that the registered RequestFilter works as expected (sets the right HTTP headers)

    assertThat(authFilter[0]).isInstanceOf(RequestFilter.class);

    Map<String, Set<String>> map = new HashMap<>();
    RequestContext context =
        new RequestContext(map, URI.create("http://localhost/"), Method.GET, null);
    authFilter[0].filter(context);

    assertThat(map)
        .containsKey("Authorization")
        .containsKey("X-Amz-Date")
        .extracting("Authorization", InstanceOfAssertFactories.iterable(String.class))
        .first(InstanceOfAssertFactories.STRING)
        .containsSequence(expectedRegion)
        .containsSequence(expectedKeyId)
        .containsSequence("Signature=")
        .containsSequence("Credential=");
  }
}
