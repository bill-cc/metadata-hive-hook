package org.billcc.lineage.hive.hook.exceptions;

/**
 * Hive hook Exception class.
 */
public class HiveHookException extends Exception {

	private static final long serialVersionUID = 1L;

	public HiveHookException() {
    }

    public HiveHookException(String message) {
        super(message);
    }

    public HiveHookException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveHookException(Throwable cause) {
        super(cause);
    }

    public HiveHookException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
