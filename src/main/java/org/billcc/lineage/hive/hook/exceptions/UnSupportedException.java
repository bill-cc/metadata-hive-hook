package org.billcc.lineage.hive.hook.exceptions;

public class UnSupportedException extends RuntimeException {

	private static final long serialVersionUID = -5588025121452725145L;

	public UnSupportedException(String message, Throwable cause) {
		super(message, cause);
	}

	public UnSupportedException(String message) {
		super(message);
	}

	public UnSupportedException(Throwable cause) {
		super(cause);
	}
}
