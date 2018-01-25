package pcosta.kafka.api;

/**
 * This interface defines a base contract for message filtering.
 * Filters are used by the message receiver or producer in order to support filtering of messages.
 *
 * @author Pedro Costa
 */
public interface MessageFilter {

    /**
     * Applies this filter to specified message
     *
     * @param topic   the message source/target as complementary information
     *                (depending on whether the filter is being applied to a receiver or sender, respectively)
     * @param msgType the message type
     * @return {@code true} if the given message shall be filtered and not delivered to the application, {@code false}
     * otherwise.
     */
    boolean filter(final String topic, final Class<?> msgType);

    /**
     * Returns whether or not this filter is currently enabled
     *
     * @return {@code true} if the filter is enabled, {@code false} otherwise.
     */
    boolean isEnabled();

    /**
     * Enables this filter.
     * If already enabled, this action takes no effect.
     */
    void enable();

    /**
     * Disables this filter.
     * If already disabled, this action takes no effect.
     */
    void disable();
}
