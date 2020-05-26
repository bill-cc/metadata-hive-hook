package org.billcc.lineage.hive.hook.events;

import org.apache.hadoop.hive.ql.hooks.Entity;
import org.billcc.hive.hook.HiveHookContext;
import org.billcc.lineage.hive.hook.entity.HiveEntity;

import java.util.ArrayList;
import java.util.List;

public class DropDatabase extends BaseHiveEvent {
    public DropDatabase(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() throws Exception {
    	HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }

    public HiveEntity getEntity() throws Exception {
        HiveEntity ret = context.createHiveEntity();
        ret.setTypeName(HIVE_TYPE_DB);

        List<String> dropTables = null;
        List<String> dropDbs = null;
        for (Entity entity : getHiveContext().getOutputs()) {
            if (entity.getType() == Entity.Type.DATABASE) {
                String dbQName = getQualifiedName(entity.getDatabase());
                if(dropDbs == null) {
                	dropDbs = new ArrayList<>();
                	ret.setAttribute(HiveEntity.KEY_DROP_DATABASE, dropDbs);
                }
                dropDbs.add(dbQName);
            } else if (entity.getType() == Entity.Type.TABLE) {
                String tblQName = getQualifiedName(entity.getTable());
                if(dropTables == null) {
                	dropTables = new ArrayList<>();
                	ret.setAttribute(HiveEntity.KEY_DROP_TABLE, dropTables);
                }
                dropTables.add(tblQName);
            }
        }
        return ret;
    }
}
