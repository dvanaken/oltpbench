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

import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.CPU1;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.CPU2;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.IO1;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.IO2;
import com.oltpbenchmark.types.TransactionStatus;

public class ResourceStresserWorker extends Worker<ResourceStresserBenchmark> {

    public ResourceStresserWorker(ResourceStresserBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
    	ResourceStresserBenchmark benchmarkModule = this.getBenchmarkModule();
        if (nextTrans.getProcedureClass().equals(CPU1.class)) {
            cpu1Transaction(benchmarkModule.getCpu1RecursiveDepth());
        } else if (nextTrans.getProcedureClass().equals(CPU2.class)) {
            cpu2Transaction(benchmarkModule.getCpu2RecursiveDepth());
        } else if (nextTrans.getProcedureClass().equals(IO1.class)) {
            io1Transaction();
        } else if (nextTrans.getProcedureClass().equals(IO2.class)) {
            io2Transaction();
        }
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }

    private void io1Transaction() throws SQLException {
        IO1 proc = this.getProcedure(IO1.class);
        assert (proc != null);
        proc.run(conn);
    }

    private void io2Transaction() throws SQLException {
        IO2 proc = this.getProcedure(IO2.class);
        assert (proc != null);
        proc.run(conn);
    }

    private void cpu1Transaction(int recursiveDepth) throws SQLException {
        CPU1 proc = this.getProcedure(CPU1.class);
        assert (proc != null);
        proc.run(conn, recursiveDepth);
    }

    private void cpu2Transaction(int recursiveDepth) throws SQLException {
        CPU2 proc = this.getProcedure(CPU2.class);
        assert (proc != null);
        proc.run(conn, recursiveDepth);
    }
}
