package com.oltpbenchmark.benchmarks.resourcestresser;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class ResourceStresserLoader extends Loader<ResourceStresserBenchmark> {
	
    private static final Logger LOG = Logger.getLogger(ResourceStresserLoader.class);

	public ResourceStresserLoader(ResourceStresserBenchmark benchmark, Connection conn) {
		super(benchmark, conn);
	}

	@Override
	public List<LoaderThread> createLoaderTheads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();
        threads.add(new LoaderThread() {
        	@Override
        	public void load(Connection conn) throws SQLException {
        		loadTable(conn, ResourceStresserConstants.TABLENAME_IO1TABLE);
        	}
        });
        threads.add(new LoaderThread() {
        	@Override
        	public void load(Connection conn) throws SQLException {
        		loadTable(conn, ResourceStresserConstants.TABLENAME_IO2TABLE2);
        	}
        });
        return (threads);
	}
	
	private void loadTable(Connection conn, String tableName) throws SQLException {
		Table catalog_tbl = this.benchmark.getTableCatalog(tableName);
		assert (catalog_tbl != null);

		if (LOG.isDebugEnabled()) LOG.debug("Start loading " + tableName);
		String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType().shouldEscapeNames());
        PreparedStatement stmt = conn.prepareStatement(sql);
    	if (tableName.equals(ResourceStresserConstants.TABLENAME_IO1TABLE) ||
    			tableName.equals(ResourceStresserConstants.TABLENAME_IO2TABLE2)) {
    		stmt.setInt(1, 1);
    		stmt.executeUpdate();
    		conn.commit();
    	}
        stmt.close();
        if (LOG.isDebugEnabled()) LOG.debug("Finished loading " + tableName);
        return;
	}

}
