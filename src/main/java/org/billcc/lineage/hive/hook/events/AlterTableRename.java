package org.billcc.lineage.hive.hook.events;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.billcc.hive.hook.HiveHookContext;
import org.billcc.lineage.hive.hook.entity.HiveEntity;

import java.util.Map;

public class AlterTableRename extends BaseHiveEvent {
    public AlterTableRename(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() throws Exception {
    	HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }
    
    public HiveEntity getEntity() throws Exception {
    	HiveEntity ret = context.createHiveEntity();
    	ret.setTypeName(HIVE_TYPE_TABLE);

        if (CollectionUtils.isEmpty(getHiveContext().getInputs())) {
            return ret;
        }

        Table oldTable  = getHiveContext().getInputs().iterator().next().getTable();
        Table newTable = null;

        if (CollectionUtils.isNotEmpty(getHiveContext().getOutputs())) {
            for (WriteEntity entity : getHiveContext().getOutputs()) {
                if (entity.getType() == Entity.Type.TABLE) {
                    newTable = entity.getTable();

                    //Hive sends with both old and new table names in the outputs which is weird. 
                    //So skipping that with the below check
                    if (StringUtils.equalsIgnoreCase(newTable.getDbName(), oldTable.getDbName()) && 
                    	StringUtils.equalsIgnoreCase(newTable.getTableName(), oldTable.getTableName())) {
                        newTable = null;
                        continue;
                    }

                    newTable = getHive().getTable(newTable.getDbName(), newTable.getTableName());

                    break;
                }
            }
        }

        if (newTable == null) {
            return ret;
        }

        Map<String, Object> oldTableEntity = toTableEntity(oldTable);
        Map<String, Object> renamedTableEntity = toTableEntity(newTable);

        // set previous name as the alias
        renamedTableEntity.put(ATTRIBUTE_ALIASES, oldTable.getTableName());
        
        ret.setAttribute(HiveEntity.KEY_TABLE_OLD, oldTableEntity);
        ret.setAttribute(HiveEntity.KEY_TABLE_RENAMED, renamedTableEntity);

        return ret;
    }
}
