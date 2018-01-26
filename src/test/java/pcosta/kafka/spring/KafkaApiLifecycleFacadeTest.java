package pcosta.kafka.spring;

import pcosta.kafka.api.MessagingException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the {@link KafkaApiLifecycleFacade} class.
 *
 * @author Pedro Costa
 */
public class KafkaApiLifecycleFacadeTest {

    // the bootstrap
    private KafkaApiBootstrap bootstrap;

    // the lifecycle facade
    private KafkaApiLifecycleFacade facade;

    @Before
    public void setup() {
        bootstrap = mock(KafkaApiBootstrap.class);
        facade = new KafkaApiLifecycleFacade(bootstrap);
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
