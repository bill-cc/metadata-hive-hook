package org.billcc.lineage.hive.hook.notification;

import org.apache.commons.configuration.Configuration;
import org.billcc.hive.hook.ApplicationProperties;

/**
 * 
 * To create notification instance
 * 
 * @author chengchao
 *
 */
public class NotificationProvider {
    private static KafkaNotification kafka;

    public static KafkaNotification get() {
        if (kafka == null) {
            try {
                Configuration applicationProperties = ApplicationProperties.get();
                kafka = new KafkaNotification(applicationProperties);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return kafka;
    }
}
