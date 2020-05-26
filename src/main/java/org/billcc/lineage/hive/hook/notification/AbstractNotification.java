package org.billcc.lineage.hive.hook.notification;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.configuration.Configuration;
import org.billcc.lineage.hive.hook.exceptions.HiveHookException;
import org.billcc.lineage.hive.hook.exceptions.NotificationException;

import java.util.Arrays;
import java.util.List;

/**
 * Abstract notification interface implementation.
 */
public abstract class AbstractNotification implements NotificationInterface {
	
	//// each char can encode upto 4 bytes in UTF-8
    public static final int MAX_BYTES_PER_CHAR = 4;

    public AbstractNotification(Configuration applicationProperties) throws HiveHookException {
    }

    @VisibleForTesting
    protected AbstractNotification() {
    }

    @Override
    public void send(List<String> messages) throws NotificationException {
        sendInternal(messages);
    }

    @Override
    public void send(String... messages) throws NotificationException {
        send(Arrays.asList(messages));
    }

    /**
     * Send the given messages.
     *
     * @param type      the message type
     * @param messages  the array of messages to send
     *
     * @throws NotificationException if an error occurs while sending
     */
    protected abstract void sendInternal(List<String> messages) throws NotificationException;
}
