/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.api.collectors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.types.DatabaseType;

public class DBParameterCollectorGen {

    public static DBParameterCollector getCollector(DatabaseType dbType, String dbUrl,
                                                    String username, String password) throws SQLException {
        Connection conn = DriverManager.getConnection(dbUrl, username, password);
        Catalog.setSeparator(conn);
        return getCollector(dbType, conn);
    }

    public static DBParameterCollector getCollector(DatabaseType dbType, Connection conn) {
        DBParameterCollector collector;

        if (dbType == DatabaseType.MYSQL || dbType == DatabaseType.MEMSQL) {
            collector = new MySQLCollector(conn);

        } else if (dbType == DatabaseType.MYROCKS) {
            collector = new MyRocksCollector(conn);

	    } else if (dbType == DatabaseType.POSTGRES) {
            collector = new PostgresCollector(conn);

        } else {
            collector = new DBCollector();

        }
        return collector;
    }
}
