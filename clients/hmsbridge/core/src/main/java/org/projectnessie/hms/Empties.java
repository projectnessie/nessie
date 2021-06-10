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
package org.projectnessie.hms;

import java.util.Collections;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;

public class Empties {

  /** Default privs. */
  public static PrincipalPrivilegeSet privSet() {
    return new PrincipalPrivilegeSet();
  }

  /** Default col stats. */
  public static ColumnStatistics colStats() {
    return new ColumnStatistics();
  }

  /** Default catalog. */
  public static Catalog defaultCatalog() {
    Catalog c = new Catalog();
    c.setName("hive");
    c.setLocationUri("nessie");
    return c;
  }

  /** Default event count. */
  public static NotificationEventsCountResponse eventCount() {
    NotificationEventsCountResponse n = new NotificationEventsCountResponse();
    n.setEventsCount(0);
    return n;
  }

  /** Default events list. */
  public static NotificationEventResponse event() {
    NotificationEventResponse n = new NotificationEventResponse();
    n.setEvents(Collections.emptyList());
    return n;
  }

  /** Default event id. */
  public static CurrentNotificationEventId eventId() {
    CurrentNotificationEventId n = new CurrentNotificationEventId();
    n.setEventId(0);
    return n;
  }
}
