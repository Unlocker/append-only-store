package ru.unlocker.append.only.store;

/**
 * Business logic exception
 *
 * @author unlocker
 */
public class AppendOnlyStoreException extends Exception {

    public AppendOnlyStoreException(String message) {
        super(message);
    }

    public AppendOnlyStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public AppendOnlyStoreException(Throwable cause) {
        super(cause);
    }

}
