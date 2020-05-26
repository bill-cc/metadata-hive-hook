package org.billcc.lineage.hive.hook.events;

import org.billcc.hive.hook.HiveHookContext;
import org.billcc.lineage.hive.hook.entity.HiveEntity;

public class AlterTable extends CreateTable {
    public AlterTable(HiveHookContext context) {
        super(context, true);
    }

    @Override
    public String getNotificationMessages() throws Exception {
    	HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }
}
