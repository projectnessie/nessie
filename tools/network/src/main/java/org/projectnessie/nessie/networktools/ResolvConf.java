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
package org.projectnessie.nessie.networktools;

// Code mostly copied from io.netty.resolver.dns.ResolvConf, but with the addition to extract
// the 'search' option values.
//
// Marker for Nessie LICENSE file - keep it
// CODE_COPIED_TO_NESSIE

import static java.util.Collections.unmodifiableList;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Looks up the {@code nameserver}s and {@code search} domains from the {@code /etc/resolv.conf}
 * file, intended for Linux and macOS.
 */
public final class ResolvConf {
  private final List<InetSocketAddress> nameservers;
  private final List<String> searchList;

  /**
   * Reads from the given reader and extracts the {@code nameserver}s and {@code search} domains
   * using the syntax of the {@code /etc/resolv.conf} file, see {@code man resolv.conf}.
   *
   * @param reader contents of {@code resolv.conf} are read from this {@link BufferedReader}, up to
   *     the caller to close it
   */
  public static ResolvConf fromReader(BufferedReader reader) throws IOException {
    return new ResolvConf(reader);
  }

  /**
   * Reads the given file and extracts the {@code nameserver}s and {@code search} domains using the
   * syntax of the {@code /etc/resolv.conf} file, see {@code man resolv.conf}.
   */
  public static ResolvConf fromFile(String file) throws IOException {
    try (FileReader fileReader = new FileReader(file);
        BufferedReader reader = new BufferedReader(fileReader)) {
      return fromReader(reader);
    }
  }

  /**
   * Returns the {@code nameserver}s and {@code search} domains from the {@code /etc/resolv.conf}
   * file. The file is only read once during the lifetime of this class.
   */
  public static ResolvConf system() {
    ResolvConf resolvConv = ResolvConf.ResolvConfLazy.machineResolvConf;
    if (resolvConv != null) {
      return resolvConv;
    }
    throw new IllegalStateException("/etc/resolv.conf could not be read");
  }

  private ResolvConf(BufferedReader reader) throws IOException {
    List<InetSocketAddress> nameservers = new ArrayList<>();
    List<String> searchList = new ArrayList<>();
    String ln;
    while ((ln = reader.readLine()) != null) {
      ln = ln.trim();
      if (ln.isEmpty()) {
        continue;
      }

      if (ln.startsWith("nameserver")) {
        ln = ln.substring("nameserver".length()).trim();
        nameservers.add(new InetSocketAddress(ln, 53));
      }
      if (ln.startsWith("search")) {
        ln = ln.substring("search".length()).trim();
        searchList.addAll(Arrays.asList(ln.split(" ")));
      }
    }
    this.nameservers = unmodifiableList(nameservers);
    this.searchList = unmodifiableList(searchList);
  }

  public List<InetSocketAddress> getNameservers() {
    return nameservers;
  }

  public List<String> getSearchList() {
    return searchList;
  }

  private static final class ResolvConfLazy {
    static final ResolvConf machineResolvConf;

    static {
      ResolvConf resolvConf;
      try {
        resolvConf = ResolvConf.fromFile("/etc/resolv.conf");
      } catch (IOException e) {
        resolvConf = null;
      }
      machineResolvConf = resolvConf;
    }
  }
}
