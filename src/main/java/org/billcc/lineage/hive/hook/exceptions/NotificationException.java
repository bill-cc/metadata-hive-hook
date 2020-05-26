package org.billcc.lineage.hive.hook.exceptions;

import java.util.List;

/**
 * Exception from notification.
 */
public class NotificationException extends Exception {
	private static final long serialVersionUID = 1L;
	
	private List<String> failedMessages;

    public NotificationException(Exception e) {
        super(e);
    }

    public NotificationException(Exception e, List<String> failedMessages) {
        super(e);
        this.failedMessages = failedMessages;
    }

    public List<String> getFailedMessages() {
        return failedMessages;
    }
}
