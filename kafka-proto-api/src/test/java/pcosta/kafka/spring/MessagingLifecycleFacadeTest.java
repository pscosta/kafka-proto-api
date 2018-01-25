package pcosta.kafka.spring;

import pcosta.kafka.api.MessagingException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the {@link MessagingLifecycleFacade} class.
 *
 * @author Pedro Costa
 */
public class MessagingLifecycleFacadeTest {

    // the bootstrap
    private KafkaApiBootstrap bootstrap;

    // the lifecycle facade
    private MessagingLifecycleFacade facade;

    @Before
    public void setup() {
        bootstrap = mock(KafkaApiBootstrap.class);
        facade = new MessagingLifecycleFacade(bootstrap);
    }

    @Test
    public void test_start() throws MessagingException {
        final ApplicationContext context = mock(ApplicationContext.class);
        facade.start(context);
        verify(bootstrap, times(1)).start(eq(context));
    }

    @Test
    public void test_stop() throws MessagingException {
        final ApplicationContext context = mock(ApplicationContext.class);
        facade.stop(context);
        verify(bootstrap, times(1)).stopListeners(eq(context));
    }

}
