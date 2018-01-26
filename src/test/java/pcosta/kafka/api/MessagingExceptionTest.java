package pcosta.kafka.api;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for the {@link MessagingException} class.
 *
 * @author Pedro Costa
 */
public class MessagingExceptionTest {

    @Test(expected = MessagingException.class)
    public void test_defaultInitialization() throws MessagingException {
        final MessagingException e = new MessagingException("Some cause");
        assertEquals("Some cause", e.getMessage());
        assertNull(e.getCause());
        throw e;
    }

    @Test(expected = MessagingException.class)
    public void test_causeInitialization() throws MessagingException {
        final Throwable cause = new RuntimeException("RuntimeException cause");
        final MessagingException e = new MessagingException("Some cause", cause);
        assertEquals("Some cause", e.getMessage());
        assertEquals(cause, e.getCause());
        throw e;
    }
}
