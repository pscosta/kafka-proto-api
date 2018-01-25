package pcosta.kafka.internal;

import pcosta.kafka.api.PlatformError;

/**
 * The platform error implementation
 *
 * @author Pedro Costa
 */
class PlatformErrorImpl implements PlatformError {

    // the error description
    private final String errorDescription;

    // the original error cause
    private final Throwable cause;

    /**
     * Default error constructor
     *
     * @param errorDescription the error description
     * @param cause            the original cause of the error
     */
    PlatformErrorImpl(final String errorDescription, final Throwable cause) {
        this.errorDescription = errorDescription;
        this.cause = cause;
    }

    @Override
    public String getErrorDescription() {
        return errorDescription;
    }

    @Override
    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return "PlatformErrorImpl{" +
                "errorDescription='" + errorDescription + '\'' +
                '}';
    }
}
