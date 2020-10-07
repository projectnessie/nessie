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

package com.dremio.nessie.gradle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.junit.Assert;
import org.junit.Test;

public class TestURL {

  @Test
  public void testUrl() {
    URL url;

    try {
      // get URL content

      String a="http://localhost:19120/api/v1/trees/tree/main";
      url = new URL(a);
      URLConnection conn = url.openConnection();

      // open the stream and put it into BufferedReader
      BufferedReader br = new BufferedReader(
        new InputStreamReader(conn.getInputStream()));

      String inputLine;
      while ((inputLine = br.readLine()) != null) {
        System.out.println(inputLine);
      }
      br.close();

      System.out.println("Done");

    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }

  }
}
