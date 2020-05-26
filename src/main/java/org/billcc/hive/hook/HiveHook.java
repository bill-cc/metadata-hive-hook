package org.billcc.hive.hook;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.billcc.lineage.hive.hook.events.*;
import org.billcc.lineage.hive.hook.notification.NotificationInterface;
import org.billcc.lineage.hive.hook.notification.NotificationProvider;
import org.billcc.lineage.hive.hook.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * The class implements ExecuteWithHookContext super class
 * 
 * @author Bill cheng
 *
 */
public class HiveHook implements ExecuteWithHookContext {
    private static final Logger LOG = LoggerFactory.getLogger(HiveHook.class);

    protected static NotificationInterface notificationInterface;
    private static final Map<String, HiveOperation> OPERATION_MAP = new HashMap<>();
    private final JsonMapper jsonMapper;

    static {
    	notificationInterface = NotificationProvider.get();
        for (HiveOperation hiveOperation : HiveOperation.values()) {
            OPERATION_MAP.put(hiveOperation.getOperationName(), hiveOperation);
        }
    }

    public HiveHook() {
    	this.jsonMapper = new JsonMapper();
    }

    @Override
    public void run(HookContext hookContext) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("HiveHook run() " + hookContext.getOperationName());
        }

        try {
            HiveOperation oper = OPERATION_MAP.get(hookContext.getOperationName());
            HiveHookContext context = new HiveHookContext(oper, hookContext, jsonMapper);

            BaseHiveEvent event = null;

            switch (oper) {
                case CREATEDATABASE:
                    event = new CreateDatabase(context);
                break;

                case DROPDATABASE:
                    event = new DropDatabase(context);
                break;

                case ALTERDATABASE:
                case ALTERDATABASE_OWNER:
                    event = new AlterDatabase(context);
                break;

                case CREATETABLE:
                    event = new CreateTable(context, true);
                break;

                case DROPTABLE:
                case DROPVIEW:
                    event = new DropTable(context);
                break;

                case ALTERTABLE_ADDPARTS:
                case CREATETABLE_AS_SELECT:
                case CREATEVIEW:
                case ALTERVIEW_AS:
                case LOAD:
                case EXPORT:
                case IMPORT:
                case QUERY:
                case TRUNCATETABLE:
                    event = new CreateHiveProcess(context);
                break;

                case ALTERTABLE_DROPPARTS:
                case ALTERTABLE_FILEFORMAT:
                case ALTERTABLE_CLUSTER_SORT:
                case ALTERTABLE_BUCKETNUM:
                case ALTERTABLE_PROPERTIES:
                case ALTERVIEW_PROPERTIES:
                case ALTERTABLE_SERDEPROPERTIES:
                case ALTERTABLE_SERIALIZER:
                case ALTERTABLE_ADDCOLS:
                case ALTERTABLE_REPLACECOLS:
                case ALTERTABLE_PARTCOLTYPE:
                case ALTERTABLE_LOCATION:
                    event = new AlterTable(context);
                break;

                case ALTERTABLE_RENAME:
                case ALTERVIEW_RENAME:
                    event = new AlterTableRename(context);
                break;

                case ALTERTABLE_RENAMECOL:
                    event = new AlterTableRenameCol(context);
                break;
                
                case SWITCHDATABASE:
                case SHOWDATABASES:
                case SHOWTABLES:
                case SHOW_CREATETABLE:
                case SHOWCOLUMNS:
                case SHOWPARTITIONS:
                case SHOWFUNCTIONS:
                case SHOW_TABLESTATUS:
                case SHOW_TBLPROPERTIES:
                case SHOWLOCKS:
                case DESCDATABASE:
                case DESCTABLE:
                case DESCFUNCTION:
                	if(LOG.isDebugEnabled()) {
                		LOG.debug("HiveHook run(), process operation {}", hookContext.getOperationName());
                	}
                break;

                default:
                    event = new DefaultEvent(context);
                break;
            }
            
            if(event != null) {
	            String message = event.getNotificationMessages();
	            if(StringUtils.isNotBlank(message)) {
	            	notificationInterface.send(message);
	            }
            }
        } catch (Throwable t) {
            LOG.error("HiveHook run(), failed to process operation {}", hookContext.getOperationName(), t);
        }
    }
}
