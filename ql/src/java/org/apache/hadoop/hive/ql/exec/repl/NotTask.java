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
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;

public class NotTask extends Task<NotWork> implements Serializable {
    private static final long serialVersionUID = 1L;
    private Logger LOG = LoggerFactory.getLogger(AckTask.class);

    @Override
    public int execute() {
        try {
            HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
            long currentNotificationID = metaStoreClient.getCurrentNotificationEventId().getEventId();
            Path notificationPath = work.getNotificationFilePath();
            Utils.writeOutput(String.valueOf(currentNotificationID), notificationPath, conf);
            LOG.info("Created Notification file : {} ", notificationPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public StageType getType() {
        return StageType.ACK;
    }

    @Override
    public String getName() {
        return "NOT_TASK";
    }

    @Override
    public boolean canExecuteInParallel() {
        // NOTIFICATION_TASK must be executed only when all its parents are done with execution.
        return false;
    }
}
