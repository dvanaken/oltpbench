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


package com.oltpbenchmark.benchmarks.resourcestresser;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.CPUMandelbrot;

public class ResourceStresserBenchmark extends BenchmarkModule {
	private static final Logger LOG = Logger.getLogger(ResourceStresserBenchmark.class);

    private static final int CPU_DEFAULT_RECURSIVE_DEPTH = 100;
    private static final int CPU_MIN_RECURSIVE_DEPTH = 1;
    private static final int CPU_MAX_RECURSIVE_DEPTH = 50000;
    private static final int IO_DEFAULT_BINARY_SIZE_KB = 2;
    private static final int IO_MIN_BINARY_SIZE_KB = 1;
    private static final int IO_MAX_BINARY_SIZE_KB = 100000;

	private final int cpuMandelbrotRecursiveDepth;
	private final int cpuMD5RecursiveDepth;
	private final int ioBinarySizekB;

	public ResourceStresserBenchmark(WorkloadConfiguration workConf) {
		super("resourcestresser", workConf, true);

		XMLConfiguration xml = workConf.getXmlConfig();
		this.cpuMandelbrotRecursiveDepth = this.resolveWorkConfParam(xml, "cpuMandelbrotRecursiveDepth",
				CPU_MIN_RECURSIVE_DEPTH, CPU_MAX_RECURSIVE_DEPTH, CPU_DEFAULT_RECURSIVE_DEPTH);
		LOG.info("Setting CPU-Mandelbrot recursive depth to " + this.cpuMandelbrotRecursiveDepth + ".");

		this.cpuMD5RecursiveDepth = this.resolveWorkConfParam(xml, "cpuMD5RecursiveDepth",
				CPU_MIN_RECURSIVE_DEPTH, CPU_MAX_RECURSIVE_DEPTH, CPU_DEFAULT_RECURSIVE_DEPTH);
		LOG.info("Setting CPU-MD5Hash recursive depth to " + this.cpuMD5RecursiveDepth + ".");

		this.ioBinarySizekB = this.resolveWorkConfParam(xml, "ioBinarySizekB",
				IO_MIN_BINARY_SIZE_KB, IO_MAX_BINARY_SIZE_KB, IO_DEFAULT_BINARY_SIZE_KB);
		LOG.info("Setting IO blob size (bytes) to " + this.ioBinarySizekB + ".");

		this.resetTables();
	}

	public int getCPUMandelbrotRecursiveDepth() {
		return this.cpuMandelbrotRecursiveDepth;
	}

	public int getCPUMD5RecursiveDepth() {
		return this.cpuMD5RecursiveDepth;
	}

	public int getIOBinarySizeBytes() {
		return this.ioBinarySizekB;
	}

	@Override
	protected Package getProcedurePackageImpl() {
	    return CPUMandelbrot.class.getPackage();
	}

	@Override
	protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
		List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
		for (int i = 0; i < workConf.getTerminals(); ++i) {
			workers.add(new ResourceStresserWorker(this, i));
		} // FOR
		return workers;
	}

	@Override
	protected Loader<ResourceStresserBenchmark> makeLoaderImpl(Connection conn) throws SQLException {
		return new ResourceStresserLoader(this, conn);
	}

	private int resolveWorkConfParam(XMLConfiguration xml, String param, int min_val,
			int max_val, int default_val) {
		int val;
		if (xml != null && xml.containsKey(param)) {
			val = xml.getInt(param);
			if (val < min_val) {
				val = min_val;
			} else if (val > max_val) {
				val = max_val;
			}
        } else {
        	val = default_val;
        }
		return val;
	}

	private boolean tableExists(Connection conn, String tableName) throws SQLException {
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet rs = meta.getTables(null, null, tableName, null);
		boolean exists = rs.next();
		rs.close();
		return exists;
	}

	private void resetTables() {
		// Reset the I/O tables before the benchmark starts. We do this because the
		// I/O procedures just do a bunch of insertions and we always want to start
		// with empty tables.
    	String[] truncateTables = {
			ResourceStresserConstants.TABLENAME_IOINTEXPONENTIAL,
			ResourceStresserConstants.TABLENAME_IOINT,
			ResourceStresserConstants.TABLENAME_IOBINARY
    	};
		Connection conn;
		try {
			conn = this.makeConnection();
			conn.setAutoCommit(false);
			Statement stmt = conn.createStatement();
			for (String tableName : truncateTables) {
				if (this.tableExists(conn, tableName)) {
					stmt.execute("TRUNCATE " + tableName);
				}
			}
			stmt.execute("INSERT INTO " + ResourceStresserConstants.TABLENAME_IOINTEXPONENTIAL +
					" SELECT * FROM " + ResourceStresserConstants.TABLENAME_IOINTSTORE);
			stmt.executeQuery("SELECT truncate_if_exists_lo('" +
					ResourceStresserConstants.TABLENAME_IOBLOB + "', 'val')");
			stmt.close();
			conn.commit();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
