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

package com.dremio.nessie.serverless;

import com.dremio.nessie.auth.User;
import com.dremio.nessie.auth.UserService;
import java.util.List;
import java.util.Optional;

/**
 * This Service is unused but currently must exist and be started for the Binder to work.
 */
public class IamUserService implements UserService {

  @Override
  public String authorize(String login, String password) {
    throw new UnsupportedOperationException();
  }

  @Override
  public User validate(String token) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<User> fetch(String username) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<User> fetchAll() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void create(User user) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void update(User user) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(String user) {
    throw new UnsupportedOperationException();
  }
}
