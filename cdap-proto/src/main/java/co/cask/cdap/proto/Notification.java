/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.proto;

import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Notification for events, such as cron expression triggering or data being added to a dataset.
 */
public class Notification {
  /**
   * The type of the notification.
   */
  public enum Type {
    TIME,
    PARTITION
  }

  private final Type notificationType;
  private final Map<String, String> properties;

  public Notification(Type notificationType, Map<String, String> properties) {
    this.notificationType = notificationType;
    this.properties = properties;
  }

  public Type getNotificationType() {
    return notificationType;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Notification that = (Notification) o;

    return Objects.equals(notificationType, that.notificationType)
      && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(notificationType, properties);
  }

  @Nullable
  public String getNotificationKey() {
    if (!Type.PARTITION.equals(triggerType)) {
      return null;
    }
    return String.format("PARTITION:%s", properties.get("datasetId"));
  }
}
