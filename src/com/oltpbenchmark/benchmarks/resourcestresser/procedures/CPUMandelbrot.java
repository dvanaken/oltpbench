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

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class CPUMandelbrot extends Procedure {
	
	public final SQLStmt cpuSelect = new SQLStmt(
			"WITH RECURSIVE " +
            "x(i) " +
            "AS ( " +
                "VALUES(0) " +
            "UNION ALL " +
                "SELECT i + 1 FROM x WHERE i < ? " +
            "), " +
            "Z(Ix, Iy, Cx, Cy, X, Y, I) " +
            "AS ( " +
                "SELECT Ix, Iy, X::FLOAT, Y::FLOAT, X::FLOAT, Y::FLOAT, 0 " +
                "FROM " +
                    "(SELECT -2.2 + 0.031 * i, i FROM x) AS xgen(x,ix) " +
                "CROSS JOIN " +
                    "(SELECT -1.5 + 0.031 * i, i FROM x) AS ygen(y,iy) " +
                "UNION ALL " +
                "SELECT Ix, Iy, Cx, Cy, X * X - Y * Y + Cx AS X, Y * X * 2 + Cy, I + 1 " +
                "FROM Z " +
                "WHERE X * X + Y * Y < 16.0 " +
                "AND I < 27 " +
            "), " +
            "Zt (Ix, Iy, I) AS ( " +
                "SELECT Ix, Iy, MAX(I) AS I " +
                "FROM Z " +
                "GROUP BY Iy, Ix " +
                "ORDER BY Iy, Ix " +
            ") " +
            "SELECT MAX(Ix), MAX(Iy), MAX(I) " +
            "FROM Zt"
		);
    
    public void run(Connection conn, int recursiveDepth) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, cpuSelect);
    	stmt.setInt(1, recursiveDepth);
    	stmt.execute();
    }
}
