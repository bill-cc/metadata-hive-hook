package org.billcc.lineage.hive.hook.events;

import org.billcc.hive.hook.HiveHookContext;
import org.billcc.lineage.hive.hook.entity.HiveEntity;

public class DefaultEvent extends BaseHiveEvent {
    public DefaultEvent(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() throws Exception {
    	HiveEntity ret = context.createHiveEntity();
    	ret.setTypeName(HIVE_TYPE_DEFAULT);
        return context.toJson(ret.getResult());
    }
}
