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

package co.cask.cdap.internal.schedule.trigger;


/**
 * A Trigger that schedules a ProgramSchedule, when a certain number of partitions are added to a PartitionedFileSet.
 */
public class PartitionTrigger extends Trigger {
  private final String datasetName;
  private final int numPartitions;

  public PartitionTrigger(String datasetName, int numPartitions) {
    this.datasetName = datasetName;
    this.numPartitions = numPartitions;
  }

  public String getDatasetName() {
    return datasetName;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionTrigger that = (PartitionTrigger) o;

    if (numPartitions != that.numPartitions) {
      return false;
    }
    return datasetName.equals(that.datasetName);

  }

  @Override
  public int hashCode() {
    int result = datasetName.hashCode();
    result = 31 * result + numPartitions;
    return result;
  }

  @Override
  public String toString() {
    return "PartitionTrigger{" +
      "datasetName=" + datasetName +
      ", numPartitions=" + numPartitions +
      '}';
  }
}
