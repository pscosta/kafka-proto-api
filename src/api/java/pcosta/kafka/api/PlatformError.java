package pcosta.kafka.api;

/**
 * The platform error message definition.
 *
 * @author Pedro Costa
 */
public interface PlatformError {

    /**
     * Returns a brief description of the error that this instance represents.
     *
     * @return the error message
     */
    String getErrorDescription();

    /**
     * Returns the original cause of the error in the underlying API.
     *
     * @return the raw, original throwable of the error
     */
    Throwable getCause();
}
