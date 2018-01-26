package pcosta.kafka.internal;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for the {@link PlatformErrorImpl} class.
 *
 * @author Pedro Costa
 */
public class PlatformErrorImplTest {

    @Test
    public void test_gettersConstruction() {
        final NullPointerException cause = new NullPointerException();
        final PlatformErrorImpl error = new PlatformErrorImpl("err1", cause);
        assertEquals("err1", error.getErrorDescription());
        assertEquals(cause, error.getCause());
    }
}
