package pcosta.kafka.internal;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Pedro Costa
 * <p>
 * Unit tests for {@link PropertiesReader} class
 */
public class PropertiesReaderTest {

    // object under testing
    private PropertiesReader propertiesReader;

    @Before
    public void setUp() {
        System.setProperty(PropertiesReader.CONFIGURATION_FILE_DIR, "src/test/resources");
        this.propertiesReader = PropertiesReader.getInstance();
    }

    @After
    public void cleanup() {
        System.setProperty(PropertiesReader.CONFIGURATION_FILE_DIR, "");
    }

    @Test
    public void loadConsumerPropsSuccessful() {
        //Prepare
        final Map<String, Object> consumerProps = propertiesReader.loadConsumerProps();

        //Call
        final Properties loadedFileProperties = propertiesReader.getProperties();

        //Assert sizes
        assertEquals(5, loadedFileProperties.size());
        assertEquals(8, consumerProps.size());

        //assert the identification tags were removed
        consumerProps.forEach((key, val) -> assertTrue(!key.startsWith("sender.")));

        //assert the values were correctly overridden
        assertTrue(consumerProps.get("consumer.timeout.ms").equals("50"));
        assertTrue(consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).toString().equals("localhost:9092_test"));
        assertTrue(consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG).toString().equals("test_consumer"));
    }

    @Test
    public void loadSenderPropsSuccessful() {
        //Prepare
        final Map<String, Object> senderProps = propertiesReader.loadSenderProps();

        //Call
        final Properties loadedFileProperties = propertiesReader.getProperties();

        //Assert sizes
        assertEquals(5, loadedFileProperties.size());
        assertEquals(8, senderProps.size());

        //assert the identification tags were removed
        senderProps.forEach((key, val) -> assertTrue(!key.startsWith("sender.")));

        //assert the values were correctly overridden
        assertTrue(senderProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).toString().equals("localhost:9092_test"));
        assertTrue(senderProps.get(ConsumerConfig.GROUP_ID_CONFIG).toString().equals("test_sender"));
    }
}