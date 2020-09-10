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
package com.dremio.nessie.server;

import io.quarkus.test.junit.NativeImageTest;
import io.restassured.http.Header;

@NativeImageTest
public class ITNativeRestGit extends RestGitTest {
  private static final String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwOi8vbmVzc2llLmRyZW1pby5jb20iLCJ1cG4iOiJhZG1pbl91c2VyIiwiZ3JvdXBzIjpbImFkbWluIiwidXNlciJdLCJiaXJ0aGRhdGUiOiIyMDAxLTA3LTEzIiwiZXhwIjo5MjIzMzcyMDM2ODU0Nzc1ODA3LCJpYXQiOjE1OTk2NjUwMDgsImp0aSI6IkpqWU9wSmp6U3JwSEc1aHJCeHpub3cifQ.NfFccKTzeP0C0lzQ1rdu27SPRIdmG_gkJcKMNXHz0guTnFUIg-w4aYMbN3IVTPRWifwOCeBiWrFVhDw0wlYYx63K5kUe4Pu8JJK-DX3YXgWFnHHP9ymQvh-ThecOpEsMpEnhzwxLqGyhVRZr52BjVod8LGgj5jLO-VLSOCs4ykAgpaZaXXI0xazWV4qPKpWr8mu0nGD7ixMWSSy_pBnHGc_KCzEKxOSKVWWzBGljEXT-rBD_T0D4hFJcsaoRIscZM7EiY8oGBtlTPzMJVzt0IA_IYItc4hfU-s1qd9d-AyUBD_96wbgE3uW__f35k-6_G98qZrorVeSCtGdXNrwBXOOrFnGAJ2X97p2fho0VZxbi1EdBO1V8GR_MfKbIPRPyC54ancyaiweKUgV8GJGgYqEELi5Ot-47lseupItTHM3xBTRdr9bMSxv4dLjp1XSfpmZeH4qUfiyJTNo7xEJO890QNbZSxXGbA2PEvnh9ImJhy7aMnJiC08v_UdXyWidbufU8eYkB4JEkoup1P4E7mTWLjGUCl1YpCsf5SQT49U4tkmxe9mjBWQpaPLM7kS_qCNk1Q-dZVS7-e8pgFk4CfMuJ3yd2yVjseS_GrsDZ99iWSMoVo-ZTMOg_jBRH7Pw0vx1qtknn_-0GqnjELUsc33tLe50PBv1-CiVfJlC4cO0";
  private static final Header header = new Header("Authorization", "Bearer " + token);

  protected Header header() {
    return header;
  }
}
