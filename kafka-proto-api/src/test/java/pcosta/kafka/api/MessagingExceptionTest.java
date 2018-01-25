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
        final MessagingException e = new MessagingException("miaow");
        assertEquals("miaow", e.getMessage());
        assertNull(e.getCause());
        throw e;
    }

    @Test(expected = MessagingException.class)
    public void test_causeInitialization() throws MessagingException {
        final Throwable cause = new RuntimeException("i did it!");
        final MessagingException e = new MessagingException("miaow", cause);
        assertEquals("miaow", e.getMessage());
        assertEquals(cause, e.getCause());
        throw e;
    }
}
