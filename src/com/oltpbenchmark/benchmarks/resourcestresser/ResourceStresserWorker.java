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
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.CPUMandelbrot;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.CPUMD5;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.IOIntExponential;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.IOInt;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.IOBinary;
import com.oltpbenchmark.benchmarks.resourcestresser.procedures.IOBlob;
import com.oltpbenchmark.types.TransactionStatus;

public class ResourceStresserWorker extends Worker<ResourceStresserBenchmark> {

    public ResourceStresserWorker(ResourceStresserBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
    	ResourceStresserBenchmark benchmarkModule = this.getBenchmarkModule();
        if (nextTrans.getProcedureClass().equals(CPUMandelbrot.class)) {
            cpuMandelbrotTransaction(benchmarkModule.getCPUMandelbrotRecursiveDepth());
        } else if (nextTrans.getProcedureClass().equals(CPUMD5.class)) {
            cpuMD5Transaction(benchmarkModule.getCPUMD5RecursiveDepth());
        } else if (nextTrans.getProcedureClass().equals(IOIntExponential.class)) {
            ioIntExponentialTransaction();
        } else if (nextTrans.getProcedureClass().equals(IOInt.class)) {
            ioIntTransaction();
        } else if (nextTrans.getProcedureClass().equals(IOBinary.class)) {
            ioBinaryTransaction();
        } else if (nextTrans.getProcedureClass().equals(IOBlob.class)) {
            ioBlobTransaction();
        }
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }

    private void ioIntExponentialTransaction() throws SQLException {
        IOIntExponential proc = this.getProcedure(IOIntExponential.class);
        assert (proc != null);
        proc.run(conn);
    }

    private void ioIntTransaction() throws SQLException {
        IOInt proc = this.getProcedure(IOInt.class);
        assert (proc != null);
        proc.run(conn);
    }

    private void ioBinaryTransaction() throws SQLException {
        IOBinary proc = this.getProcedure(IOBinary.class);
        assert (proc != null);
        proc.run(conn);
    }

    private void ioBlobTransaction() throws SQLException {
        IOBlob proc = this.getProcedure(IOBlob.class);
        assert (proc != null);
        proc.run(conn);
    }

    private void cpuMandelbrotTransaction(int recursiveDepth) throws SQLException {
        CPUMandelbrot proc = this.getProcedure(CPUMandelbrot.class);
        assert (proc != null);
        proc.run(conn, recursiveDepth);
    }

    private void cpuMD5Transaction(int recursiveDepth) throws SQLException {
        CPUMD5 proc = this.getProcedure(CPUMD5.class);
        assert (proc != null);
        proc.run(conn, recursiveDepth);
    }
}
