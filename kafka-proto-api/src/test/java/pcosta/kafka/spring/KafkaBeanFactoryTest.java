package pcosta.kafka.spring;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.ListableBeanFactory;
import pcosta.kafka.api.MessagingContext;
import pcosta.kafka.api.MessagingException;
import pcosta.kafka.api.MessagingFactory;
import pcosta.kafka.internal.MessagingFactoryImpl;
import pcosta.kafka.spring.annotation.EnableMessagingBootstrap;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link KafkaBeanFactory} class.
 *
 * @author Pedro Costa
 */
public class KafkaBeanFactoryTest {

    @InjectMocks
    private KafkaBeanFactory kafkaBeanFactory;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void test_messagingFactory() {
        final MessagingFactory messagingFactory = kafkaBeanFactory.messagingFactory();
        assertNotNull(messagingFactory);
        assertEquals(MessagingFactoryImpl.class, messagingFactory.getClass());
    }

    @Test
    public void test_messagingBootstrap() {
        final MessagingBootstrap messagingBootstrap = kafkaBeanFactory.messagingBootstrap();
        assertNotNull(messagingBootstrap);
        assertEquals(MessagingBootstrap.class, messagingBootstrap.getClass());
    }

    @Test
    public void test_messagingLifecycleFacade() {
        final MessagingLifecycleFacade messagingLifecycleFacade = kafkaBeanFactory.messagingLifecycleFacade(mock(MessagingBootstrap.class));
        assertNotNull(messagingLifecycleFacade);
        assertEquals(MessagingLifecycleFacade.class, messagingLifecycleFacade.getClass());
    }

    @Test
    public void test_messagingContext() throws MessagingException {
        final MessagingContext context = mock(MessagingContext.class);
        final MessagingFactory factory = mock(MessagingFactory.class);
        when(factory.createContext("ADMIN")).thenReturn(context);

        final Map<String, Object> beans = new HashMap<>();
        beans.put("dummy", mock(Object.class));

        final EnableMessagingBootstrap annotation = mock(EnableMessagingBootstrap.class);
        when(annotation.topic()).thenReturn("ADMIN");

        final ListableBeanFactory beanFactory = mock(ListableBeanFactory.class);
        when(beanFactory.getBeansWithAnnotation(eq(EnableMessagingBootstrap.class))).thenReturn(beans);
        when(beanFactory.findAnnotationOnBean(eq("dummy"), eq(EnableMessagingBootstrap.class))).thenReturn(annotation);

        assertEquals(context, kafkaBeanFactory.messagingContext(factory, beanFactory));
    }
}
