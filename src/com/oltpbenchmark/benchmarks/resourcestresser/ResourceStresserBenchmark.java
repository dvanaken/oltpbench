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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.CPU1;

public class ResourceStresserBenchmark extends BenchmarkModule {
	private static final Logger LOG = Logger.getLogger(ResourceStresserBenchmark.class);

    private static final int CPU1_DEFAULT_RECURSIVE_DEPTH = 101;
    private static final int CPU2_DEFAULT_RECURSIVE_DEPTH = 1000;

	private final int cpu1RecursiveDepth;
	private final int cpu2RecursiveDepth;

	public ResourceStresserBenchmark(WorkloadConfiguration workConf) {
		super("resourcestresser", workConf, true);

		XMLConfiguration xml = workConf.getXmlConfig();
		if (xml != null && xml.containsKey("cpu1RecursiveDepth")) {
		    this.cpu1RecursiveDepth = xml.getInt("cpu1RecursiveDepth");
        } else {
        	this.cpu1RecursiveDepth = CPU1_DEFAULT_RECURSIVE_DEPTH;
        }
		if (xml != null && xml.containsKey("cpu2RecursiveDepth")) {
		    this.cpu2RecursiveDepth = xml.getInt("cpu2RecursiveDepth");
        } else {
        	this.cpu2RecursiveDepth = CPU2_DEFAULT_RECURSIVE_DEPTH;
        }
	}

	public int getCpu1RecursiveDepth() {
		return this.cpu1RecursiveDepth;
	}

	public int getCpu2RecursiveDepth() {
		return this.cpu2RecursiveDepth;
	}
	
	@Override
	protected Package getProcedurePackageImpl() {
	    return CPU1.class.getPackage();
	}
	
	@Override
	protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
		LOG.info("Setting CPU1 recursion level to " + this.cpu1RecursiveDepth + ".");
		LOG.info("Setting CPU2 recursion level to " + this.cpu2RecursiveDepth + ".");
		List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
		int numKeys = (int) (workConf.getScaleFactor() * ResourceStresserConstants.RECORD_COUNT);
		int keyRange = numKeys / workConf.getTerminals();
		// TODO: check ranges
		for (int i = 0; i < workConf.getTerminals(); ++i) {
			workers.add(new ResourceStresserWorker(this, i, numKeys, keyRange));
		} // FOR

		return workers;
	}
	
	@Override
	protected Loader<ResourceStresserBenchmark> makeLoaderImpl(Connection conn) throws SQLException {
		return new ResourceStresserLoader(this, conn);
	}
}
