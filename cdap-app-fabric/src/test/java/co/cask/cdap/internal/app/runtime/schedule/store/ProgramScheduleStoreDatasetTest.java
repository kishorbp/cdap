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

package co.cask.cdap.internal.app.runtime.schedule.store;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

public class ProgramScheduleStoreDatasetTest extends AppFabricTestBase {

  @Test
  public void checkDatasetType() throws DatasetManagementException {
    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);
    Assert.assertTrue(dsFramework.hasType(NamespaceId.SYSTEM.datasetType(ProgramScheduleStoreDataset.class.getName())));
  }
}
