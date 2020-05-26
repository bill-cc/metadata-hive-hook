package org.billcc.lineage.hive.hook.events;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.billcc.hive.hook.HiveHookContext;
import org.billcc.lineage.hive.hook.entity.HiveEntity;

public class CreateDatabase extends BaseHiveEvent {
    public CreateDatabase(HiveHookContext context) {
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
    	
        for (Entity entity : getHiveContext().getOutputs()) {
            if (entity.getType() == Entity.Type.DATABASE) {
                Database db = entity.getDatabase();

                if (db != null) {
                    db = getHive().getDatabase(db.getName());
                }
                
                if (db != null) {
                    ret.setAttribute(HIVE_TYPE_DB, toDbEntity(db));
                }
            }
        }
        return ret;
    }
}
