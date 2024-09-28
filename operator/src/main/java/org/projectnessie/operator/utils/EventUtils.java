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
package org.projectnessie.operator.utils;

import io.fabric8.kubernetes.api.model.HasMetadata;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.projectnessie.operator.events.EventReason;
import org.projectnessie.operator.exception.InvalidSpecException;

public final class EventUtils {

  private EventUtils() {}

  /**
   * A formatter that is compliant with the Kubernetes API server's expectations for the Time v1
   * type.
   *
   * <p>Kubernetes expects Time to be formatted as RFC 3339 with a time zone offset, or 'Z'. The Go
   * constant definition is:
   *
   * <pre>
   *   const RFC3339 = "2006-01-02T15:04:05Z07:00"
   * </pre>
   *
   * @see <a
   *     href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#time-v1-meta">Time
   *     v1</a>
   * @see <a href="https://pkg.go.dev/time#pkg-constants">Go time package constants</a>
   * @see <a
   *     href="https://github.com/kubernetes/apimachinery/blob/master/pkg/apis/meta/v1/time.go">Kubernetes
   *     time.go</a>
   */
  private static final DateTimeFormatter TIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");

  /**
   * A formatter that is compliant with the Kubernetes API server's expectations for the MicroTime
   * v1 type. MicroTime is a version of Time with microsecond-level precision.
   *
   * <p>Kubernetes expects MicroTime to be formatted as RFC 3339 with a fractional seconds part and
   * a time zone offset, or 'Z'. The Go constant definition is:
   *
   * <pre>
   *   const RFC3339Micro = "2006-01-02T15:04:05.000000Z07:00"
   * </pre>
   *
   * @see <a
   *     href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#microtime-v1-meta">MicroTime
   *     v1</a>
   * @see <a href="https://pkg.go.dev/time#pkg-constants">Go time package constants</a>
   * @see <a
   *     href="https://github.com/kubernetes/apimachinery/blob/master/pkg/apis/meta/v1/micro_time.go">Kubernetes
   *     micro_time.go</a>
   */
  private static final DateTimeFormatter MICRO_TIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX");

  public static String formatTime(ZonedDateTime zdt) {
    return TIME.format(zdt);
  }

  public static String formatMicroTime(ZonedDateTime zdt) {
    return MICRO_TIME.format(zdt);
  }

  public static String eventName(HasMetadata primary, EventReason reason) {
    return primary.getSingular() + "-" + primary.getMetadata().getUid() + "-" + reason;
  }

  public static EventReason reasonFromEventName(String eventName) {
    int lastDash = eventName.lastIndexOf('-');
    return EventReason.valueOf(eventName.substring(lastDash + 1));
  }

  public static EventReason errorReason(Throwable t) {
    return t instanceof InvalidSpecException ise ? ise.getReason() : EventReason.ReconcileError;
  }

  public static String formatMessage(String message, Object... args) {
    // Message is limited to 1024 characters in practice
    message = String.format(message, args);
    if (message.length() > 1024) {
      // add ellipsis to indicate that the message was truncated
      String ellipsis = "... [truncated]";
      message = message.substring(0, 1024 - ellipsis.length()) + ellipsis;
    }
    return message;
  }

  public static String getErrorMessage(Throwable t) {
    return t.getMessage() == null ? t.toString() : t.getMessage();
  }

  public static Throwable launderThrowable(
      Throwable t, Class<? extends Throwable> preferredThrowableClass) {
    Throwable t1 = t;
    do {
      if (preferredThrowableClass.isInstance(t1)) {
        return t1;
      }
      t1 = t1.getCause();
    } while (t1 != null);
    return t;
  }
}
