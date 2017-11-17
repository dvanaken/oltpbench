package com.oltpbenchmark.benchmarks.resourcestresser.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserConstants;

public class IOBlob extends Procedure {
    private static final Logger LOG = Logger.getLogger(Procedure.class);
    
    public final SQLStmt ioInsert = new SQLStmt(
        "INSERT INTO " + ResourceStresserConstants.TABLENAME_IOBLOB +
        " SELECT make_lo(val) FROM " + ResourceStresserConstants.TABLENAME_IOBINARYSTORE
    );

    public void run(Connection conn) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, ioInsert);
        int result = stmt.executeUpdate();
        if (result <= 0) {
        	LOG.warn("Inserted " + result + " tuples (expected to insert >= 1 tuples).");
        } else if (LOG.isDebugEnabled()) {
        	LOG.debug("Inserted " + result + " tuples.");
        }
    }
}
