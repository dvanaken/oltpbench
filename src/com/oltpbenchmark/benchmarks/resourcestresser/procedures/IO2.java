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

package com.oltpbenchmark.benchmarks.resourcestresser.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserConstants;

/**
 * io2Transaction deals with a table that has much smaller rows.
 * It runs a given number of updates, where each update only 
 * changes one row.
 */
public class IO2 extends Procedure {
    private static final Logger LOG = Logger.getLogger(Procedure.class);
    
    public final SQLStmt ioInsert = new SQLStmt(
        "INSERT INTO " + ResourceStresserConstants.TABLENAME_IO2TABLE1 +
        " SELECT * FROM " + ResourceStresserConstants.TABLENAME_IO2TABLE2
    );
    
    public void run(Connection conn) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, ioInsert);
        stmt.executeUpdate();
    }
}
