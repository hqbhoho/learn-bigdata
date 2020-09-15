/**
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

package com.hqbhoho.bigdata.hive.hooks;


import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.hooks.*;


import java.util.Map;
import java.util.Set;


public class LineageParseHook implements ExecuteWithHookContext {



  @Override
  public void run(HookContext hookContext) throws Exception {

    String queryStr = hookContext.getQueryPlan().getQueryStr().trim();
    System.err.println("************"+queryStr);
    System.err.println("**************************************linfo*********************************");
    LineageInfo linfo = hookContext.getLinfo();
    for (Map.Entry<LineageInfo.DependencyKey, LineageInfo.Dependency> entry :linfo.entrySet()) {
      LineageInfo.DependencyKey key = entry.getKey();

      LineageInfo.DataContainer dataContainer = key.getDataContainer();
      System.err.println(dataContainer.getPartition().getParameters());
      System.err.println(dataContainer.getTable().getPartitionKeys());
      System.err.println(dataContainer.getTable().getTableName());

      FieldSchema fieldSchema = key.getFieldSchema();
      System.err.println(fieldSchema.getName());


      LineageInfo.Dependency value = entry.getValue();
      System.err.println(value.getType());
      System.err.println(value.getBaseCols());
      System.err.println(value.getExpr());

    }

    System.err.println("**************************************Inputs*********************************");
    Set<ReadEntity> inputs = hookContext.getInputs();
    for(ReadEntity readEntity:inputs){

      System.err.println(readEntity.getDatabase());
      System.err.println(readEntity.getTable());
      System.err.println(readEntity.getAccessedColumns());

    }
    System.err.println("**************************************OutPuts*********************************");
    Set<WriteEntity> outputs = hookContext.getOutputs();
    for(WriteEntity writeEntity:outputs){
      System.err.println(writeEntity.getDatabase());
      System.err.println(writeEntity.getTable());
    }
  }
}
