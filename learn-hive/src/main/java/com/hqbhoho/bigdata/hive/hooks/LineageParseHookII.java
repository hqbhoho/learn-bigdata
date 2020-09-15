package com.hqbhoho.bigdata.hive.hooks;

import com.hqbhoho.bigdata.hive.hooks.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.*;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/21
 */
public class LineageParseHookII implements PostExecute {
    static final private Log LOG = LogFactory.getLog(LineageParseHookII.class);

    public void run(SessionState sess, Set<ReadEntity> inputs, Set<WriteEntity> outputs, LineageInfo lInfo, UserGroupInformation ugi) throws Exception {



        lInfo = sess.getLineageState().getLineageInfo();

        inputs.forEach(in-> in.getParents());


        buildLineage(inputs,outputs,lInfo);
    }

    private Optional<Table> fromEntity(Entity entity ) {
        if( entity.getType() != Entity.Type.TABLE && entity.getType() != Entity.Type.PARTITION) {
            return Optional.empty();
        }
        Table tbl = new Table(entity.getTable().getDbName(), entity.getTable().getTableName());

        if(entity instanceof ReadEntity && ((ReadEntity)entity).getAccessedColumns() != null) {
            tbl.setColumns( ((ReadEntity)entity).getAccessedColumns() );
        }
        System.err.println("Entity type " + entity.getType() + " table " + tbl.toString());
        return Optional.of(tbl);
    }

    private List<Table> fromEntities(Set<? extends Entity> entities) {
        List<Table> tables = new ArrayList<>();
        if(entities != null ) {
            for (Entity entity : entities) {
                if (entity != null) {
                    Optional<Table> opt = fromEntity(entity);



                    opt.ifPresent(tables::add);
                }
            }
        }
        return tables;
    }
    /**
     * 解析血缘关系
     * @param
     * @return
     */
    private Optional<Lineage> buildLineage(Set<ReadEntity> inputs, Set<WriteEntity> outputs, LineageInfo lInfo) {
        System.err.println("======================================");
        Lineage lineage = new Lineage();

        lineage.setSourceTables( fromEntities(inputs) );
        lineage.setTargetTables( fromEntities(outputs) );



        LineageInfo lineageInfo = lInfo;

        System.err.println("==I==" +lineageInfo.entrySet().isEmpty());
        System.err.println("==I==" +lineageInfo.entrySet().size());
        System.err.println("==I==" +lineageInfo.entrySet());

        if(lineageInfo == null) {
            return Optional.empty();
        }

        List<UpstreamsDownstream> deps = new ArrayList<>();
        for (Map.Entry<LineageInfo.DependencyKey, LineageInfo.Dependency> entry : lineageInfo.entrySet()) {
            //上游
            LineageInfo.Dependency dep = entry.getValue();
            //下游
            LineageInfo.DependencyKey depK = entry.getKey();

            if(dep == null) {
                continue;
            }

            System.err.println("Upstream " + dep + " downstream " + depK);

            DbTblCol downstream = new DbTblCol();
            downstream.setDbName( depK.getDataContainer().getTable().getDbName() );
            downstream.setTblName( depK.getDataContainer().getTable().getTableName() );
            downstream.setColumn( new Column(depK.getFieldSchema()));

            if(depK.getDataContainer().isPartition()) {
                List<FieldSchema> partitionKeys = depK.getDataContainer().getTable().getPartitionKeys();
                Partition part = depK.getDataContainer().getPartition();
                int i = 0;
                List<PartitionKeyValue> partitionKeyValues = new ArrayList<>();
                for (FieldSchema partitionKey : partitionKeys) {
                    PartitionKeyValue pkv = new PartitionKeyValue();
                    pkv.setPartitionKey( partitionKey.getName() );
                    pkv.setPartitionValue( part.getValues().get(i ++));
                    partitionKeyValues.add(pkv);
                }
                downstream.setPartition( partitionKeyValues );
            }

            List<DbTblCol> upstreams = new ArrayList<>();
            if(dep != null) {

                System.out.println(dep.getExpr());
                for (LineageInfo.BaseColumnInfo baseCol : dep.getBaseCols()) {
                    if(baseCol.getColumn() == null){
                        LOG.warn(baseCol.toString() + " has NULL FieldSchema");
                        continue;
                    }
                    DbTblCol dbTblCol = new DbTblCol();
                    dbTblCol.setDbName( baseCol.getTabAlias().getTable().getDbName());
                    dbTblCol.setTblName( baseCol.getTabAlias().getTable().getTableName());
                    dbTblCol.setColumn(  new Column(baseCol.getColumn()) );
                    upstreams.add(dbTblCol);
                }
            }
            deps.add(new UpstreamsDownstream(upstreams, downstream));
        }
        lineage.setDependencies( deps );
        System.err.println(lineage);
        return Optional.of(lineage);

    }


}
