package pcosta.kafka.internal;

import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import pcosta.kafka.api.*;

import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * @author Pedro Costa
 * <p/>
 * Units for {@link KafkaContext} class
 */
public class KafkaContextTest {

    //Object under testing
    private KafkaContext context;

    @Mock
    private PlatformErrorListener errorListener;
    @Mock
    private Collection<MessageListenerConfiguration<?>> listeners;
    @Mock
    private MessageReceiverConfiguration configuration;

    @Before
    public void setUp() {
        initMocks(this);
        // init out
        this.context = new KafkaContext();
    }

    @Test
    public void createReceiver() {
        //Prepare
        when(configuration.getErrorListener()).thenReturn(errorListener);
        when(configuration.getListeners()).thenReturn(listeners);

        //Call
        this.context.createReceiver(configuration);

        //Assert
        verify(configuration).getErrorListener();
        verify(configuration).getListeners();
    }

    @Test(expected = IllegalStateException.class)
    public void createReceiver_calledTwice() {
        //Call 1: ok
        this.context.createReceiver(configuration);

        //Call 2: fails
        this.context.createReceiver(configuration);
    }

    @Test
    public void createProducer() {
        //Prepare
        when(configuration.getErrorListener()).thenReturn(errorListener);
        when(configuration.getListeners()).thenReturn(listeners);

        //Call
        final MessageProducer producer = this.context.createProducer("string-proto", new StringSerializer(), new ProtobufSerializer());

        //Assert
        assertNotNull(producer);
    }

    @Test
    public void createProducer_calledTwice() {
        //Prepare
        when(configuration.getErrorListener()).thenReturn(errorListener);
        when(configuration.getListeners()).thenReturn(listeners);

        //Call
        final MessageProducer producer1 = this.context.createProducer("string-proto", new StringSerializer(), new ProtobufSerializer());
        final MessageProducer producer2 = this.context.createProducer("string-proto", new StringSerializer(), new ProtobufSerializer());

        //Assert
        assertNotNull(producer1);
        assertEquals(producer1, producer2);
    }

    @Test
    public void createProducer_withFilters() throws MessagingException {
        final MessageFilter filter = mock(MessageFilter.class);
        assertNotNull(context.createProducer("string-proto", new StringSerializer(), new ProtobufSerializer(), Collections.singletonList(filter)));
    }

}