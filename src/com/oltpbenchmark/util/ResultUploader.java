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

package com.oltpbenchmark.util;

import com.oltpbenchmark.Results;
import com.oltpbenchmark.api.BenchmarkModule;;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.collectors.DBParameterCollector;
import com.oltpbenchmark.api.collectors.DBParameterCollectorGen;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

public class ResultUploader {
    private static final Logger LOG = Logger.getLogger(ResultUploader.class);

    private static final String[] IGNORE_CONF = {
            "dbtype",
            "driver",
            "DBUrl",
            "username",
            "password",
            "uploadCode",
            "uploadUrl"
    };

    private Results results;
    private BenchmarkModule bench;

    private String uploadCode;
    private String uploadUrl;
    private String uploadHash;

    public ResultUploader(String uploadCode, String uploadUrl, String uploadHash,
                          Results results, BenchmarkModule bench) {

        this.uploadCode = uploadCode;
        this.uploadUrl = uploadUrl;
        this.uploadHash = uploadHash;
        this.results = results;
        this.bench = bench;
    }
    
    public static void writeBenchmarkConf(XMLConfiguration benchConf, PrintStream os) throws ConfigurationException {
        XMLConfiguration copy = (XMLConfiguration) benchConf.clone();
        for (String key: IGNORE_CONF) {
            copy.clearProperty(key);
        }
        copy.save(os);
    }

    public void writeBenchmarkConf(PrintStream os) throws ConfigurationException {
        writeBenchmarkConf(this.bench.getWorkloadConfiguration().getXmlConfig(), os);
    }

    public static String getDBVersion(BenchmarkModule bench) {
        Connection conn;
        DatabaseMetaData meta;
        int majorVersion = 0;
        int minorVersion = 0;
        String version;
        try {
            conn = bench.makeConnection();
            meta = conn.getMetaData();
            majorVersion = meta.getDatabaseMajorVersion();
            majorVersion = meta.getDatabaseMinorVersion();
            version = String.format("%s.%s", majorVersion, minorVersion);
        } catch (SQLException ex) {
            version = null;
        }
        return version;
    }

    public static void writeSummary(BenchmarkModule benchMod, Results res, PrintStream os) {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Date now = new Date();
        String benchType = benchMod.getBenchmarkName();
        WorkloadConfiguration wkldConfig = benchMod.getWorkloadConfiguration();

        Map<String, Object> summary = new TreeMap<String, Object>();
        summary.put("Current Timestamp (milliseconds)", now.getTime());
        summary.put("DBMS Type", wkldConfig.getDBType());
        summary.put("DBMS Version", getDBVersion(benchMod));
        summary.put("Benchmark Type", benchMod.getBenchmarkName());
        summary.put("Latency Distribution", res.latencyDistribution.toMap());
        summary.put("Throughput (requests/second)", res.getRequestsPerSecond());
        summary.put("Runtime (seconds)", res.getRuntimeSeconds());
        summary.put("isolation", wkldConfig.getIsolationString());
        summary.put("scalefactor", wkldConfig.getScaleFactor());
        summary.put("terminals", wkldConfig.getTerminals());
        os.println(JSONUtil.format(JSONUtil.toJSONString(summary)));
    }

    public void writeSummary(PrintStream os) {
        writeSummary(this.bench, this.results, os);
    }

    public void uploadResult(List<TransactionType> activeTXTypes, Map<String, String> resultFiles) throws ParseException {
        try {
            File expConfigFile;
            File samplesFile;
            File summaryFile;
            File paramsFile ;
            File metricsFile;
            File csvDataFile;

            DBParameterCollector collector = null;
            PrintStream confOut;

            if (resultFiles.containsKey("expconfig")) {
                expConfigFile = new File(resultFiles.get("expconfig"));
            } else {
                expConfigFile = File.createTempFile("expconfig", ".tmp");
                confOut = new PrintStream(new FileOutputStream(expConfigFile));
                writeBenchmarkConf(confOut);
                confOut.close();
            }

            if (resultFiles.containsKey("samples")) {
                samplesFile = new File(resultFiles.get("samples"));
            } else {
                samplesFile = File.createTempFile("samples", ".tmp");
                confOut = new PrintStream(new FileOutputStream(samplesFile));
                results.writeCSV2(confOut);
                confOut.close();
            }

            if (resultFiles.containsKey("summary")) {
                summaryFile = new File(resultFiles.get("summary"));
            } else {
                summaryFile = File.createTempFile("summary", ".tmp");
                confOut = new PrintStream(new FileOutputStream(summaryFile));
                writeSummary(confOut);
                confOut.close();
            }

            if (resultFiles.containsKey("params")) {
                paramsFile = new File(resultFiles.get("params"));
            } else {
                paramsFile = File.createTempFile("params", ".tmp");
                confOut = new PrintStream(new FileOutputStream(paramsFile));
                if (collector == null) collector = this.bench.createDBCollector();
                confOut.println(collector.collectParameters());
                confOut.close();
            }

            if (resultFiles.containsKey("metrics")) {
                metricsFile = new File(resultFiles.get("metrics"));
            } else {
                metricsFile = File.createTempFile("metrics", ".tmp");
                confOut = new PrintStream(new FileOutputStream(metricsFile));
                if (collector == null) collector = this.bench.createDBCollector();
    	        confOut.println(collector.collectMetrics());
                confOut.close();
            }

            if (resultFiles.containsKey("csv")) {
                csvDataFile = new File(resultFiles.get("csv") + ".gz");
            } else {
                csvDataFile = File.createTempFile("csv", ".gz");
                confOut = new PrintStream(new GZIPOutputStream(new FileOutputStream(csvDataFile)));
                results.writeAllCSVAbsoluteTiming(activeTXTypes, confOut);
                confOut.close();
            }

            CloseableHttpClient httpclient = HttpClients.createDefault();
            HttpPost httppost = new HttpPost(this.uploadUrl);

            HttpEntity reqEntity = MultipartEntityBuilder.create()
                    .addTextBody("upload_code", this.uploadCode)
                    .addTextBody("upload_hash", this.uploadHash)
                    .addPart("sample_data", new FileBody(samplesFile))
                    .addPart("raw_data", new FileBody(csvDataFile))
                    .addPart("db_parameters_data", new FileBody(paramsFile))
                    .addPart("db_metrics_data", new FileBody(metricsFile))
                    .addPart("benchmark_conf_data", new FileBody(expConfigFile))
                    .addPart("summary_data", new FileBody(summaryFile))
                    .build();

            httppost.setEntity(reqEntity);

            LOG.info("executing request " + httppost.getRequestLine());
            CloseableHttpResponse response = httpclient.execute(httppost);
            try {
                HttpEntity resEntity = response.getEntity();
                LOG.info(IOUtils.toString(resEntity.getContent()));
                EntityUtils.consume(resEntity);
            } finally {
                response.close();
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ConfigurationException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
