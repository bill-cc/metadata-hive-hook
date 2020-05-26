package org.billcc.lineage.hive.hook.notification;

import java.util.List;

import org.billcc.lineage.hive.hook.exceptions.NotificationException;

public interface NotificationInterface {
    /**
     * Send the given messages.
     *
     * @param type      the message type
     * @param messages  the messages to send
     * @param <T>       the message type
     *
     * @throws NotificationException if an error occurs while sending
     */
    void send(String... messages) throws NotificationException;

    /**
     * Send the given messages.
     *
     * @param type      the message type
     * @param messages  the list of messages to send
     * @param <T>       the message type
     *
     * @throws NotificationException if an error occurs while sending
     */
    void send(List<String> messages) throws NotificationException;

    /**
     * Shutdown any notification producers and consumers associated with this interface instance.
     */
    void close();
}
