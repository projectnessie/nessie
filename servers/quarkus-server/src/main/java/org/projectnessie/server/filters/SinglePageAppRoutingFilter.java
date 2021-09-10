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
package org.projectnessie.server.filters;

import io.quarkus.vertx.web.RouteFilter;
import io.vertx.ext.web.RoutingContext;

/** Routes UI routes to http page. */
public class SinglePageAppRoutingFilter {

  // maintain list of base paths that should serve root html.
  // must be updated as more UI paths are introduced
  private static final String[] UI_ROUTES = {"/tree"};

  @RouteFilter(100)
  void rerouteUiPaths(RoutingContext rc) {
    final String path = rc.normalizedPath();
    for (String prefix : UI_ROUTES) {
      if (path.startsWith(prefix)) {
        rc.reroute("/");
        return;
      }
    }

    // default routing
    rc.next();
  }
}
