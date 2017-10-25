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
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class CPU2 extends Procedure {

    public final SQLStmt cpuSelect = new SQLStmt(
    		"WITH RECURSIVE md5_compute(i, j) AS (" +
    				"SELECT 0, md5('0') UNION ALL " +
    				"SELECT i+1, concat(j, md5(j)) " +
    				"FROM md5_compute " +
    				"WHERE i < ?" +
    		") SELECT md5(max(j)) from md5_compute"
    );
    
    public void run(Connection conn, int recursiveDepth) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, cpuSelect);
    	stmt.setInt(1, recursiveDepth);
    	ResultSet rs = stmt.executeQuery();
    	rs.close();
    }
    
}
