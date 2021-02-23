/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.plan.Explain;

import java.io.*;

/**
 * Notification Event file dump on repl dump completion.
 *
 */
@Explain(displayName = "Notification Event File", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
public class NotWork implements Serializable {
    private static final long serialVersionUID = 1L;
    private String dumpLocation;
    private transient ReplicationMetricCollector metricCollector;
    private HiveConf hiveConf;
    public String dbName;

    public ReplicationMetricCollector getMetricCollector() {
        return metricCollector;
    }
    public long fetchSrcTxnId() throws IOException {
        FileSystem fs = new Path(dumpLocation).getFileSystem(hiveConf);
        Path openSrcTxnFile = new Path(dumpLocation, "_openTxnMetadata");
        InputStream inputStream = fs.open(openSrcTxnFile);

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        long srcTxnId = 0;
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            srcTxnId = Long.parseLong(line);
        }
        if(reader!= null) reader.close();
        return srcTxnId;
    }

    public NotWork(String dumpLocation, ReplicationMetricCollector metricCollector,
                   HiveConf hiveConf, String dbName) {
        this.metricCollector = metricCollector;
        this.dumpLocation = dumpLocation;
        this.hiveConf = hiveConf;
        this.dbName = dbName;
    }

    public String getDumpLocation(){
        return dumpLocation;
    }

}

