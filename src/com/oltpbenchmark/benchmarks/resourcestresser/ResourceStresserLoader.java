package com.oltpbenchmark.benchmarks.resourcestresser;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
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
        		loadTables(conn);
        	}
        });
        return (threads);
	}

	private void loadTables(Connection conn) throws SQLException {
		boolean escNames = this.getDatabaseType().shouldEscapeNames();
		Object[] value = { 1 };
		int[] sqlType = { Types.INTEGER };

		if (LOG.isDebugEnabled()) LOG.debug("Starting to load tables...");
		Table catalogTable = this.benchmark.getTableCatalog(
				ResourceStresserConstants.TABLENAME_IOINTEXPONENTIAL);
		this.executeInsertSQL(conn, SQLUtil.getInsertSQL(catalogTable, escNames), value, sqlType);

		catalogTable = this.benchmark.getTableCatalog(
				ResourceStresserConstants.TABLENAME_IOINTSTORE);
		this.executeInsertSQL(conn, SQLUtil.getInsertSQL(catalogTable, escNames), value, sqlType);

		String sql = "INSERT INTO " + ResourceStresserConstants.TABLENAME_IOBINARYSTORE +
				" VALUES(gen_binary_string(?))";
		value[0] = this.benchmark.getIOBinarySizeBytes();
		this.executeInsertSQL(conn, sql, value, sqlType);

		conn.commit();
		if (LOG.isDebugEnabled()) LOG.debug("ResourceStresser loader done.");
	}

	private void executeInsertSQL(Connection conn, String sql, Object[] values, int[] sqlTypes) throws SQLException {
		assert (values.length == sqlTypes.length);
		PreparedStatement stmt = conn.prepareStatement(sql);
		for (int i = 0; i < values.length; ++i) {
			stmt.setObject(i + 1, values[i], sqlTypes[i]);
			stmt.executeUpdate();
			stmt.close();
		}
	}
}
