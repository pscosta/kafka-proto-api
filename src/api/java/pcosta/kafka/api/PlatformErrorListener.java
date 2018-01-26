package pcosta.kafka.api;

/**
 * This interface defines both platform messages and error handling by extending the {@link MessageListener} contract.
 * *
 *
 * @author Pedro Costa
 */
public interface PlatformErrorListener {

    /**
     * Handles an arbitrary platform error
     *
     * @param error the platform error
     */
    void onError(final PlatformError error);
}
