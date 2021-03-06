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

package co.cask.wrangler.service.database;

import co.cask.cdap.etl.api.Destroyable;
import com.google.common.base.Throwables;

import javax.annotation.Nullable;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Cleans up JDBC drivers.
 */
public class DriverCleanup implements Destroyable {
  private final JDBCDriverShim driverShim;

  DriverCleanup(@Nullable JDBCDriverShim driverShim) {
    this.driverShim = driverShim;
  }

  public void destroy() {
    if (driverShim != null) {
      try {
        DriverManager.deregisterDriver(driverShim);
      } catch (SQLException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
