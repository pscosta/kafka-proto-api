package pcosta.kafka.internal;

import org.junit.Test;
import pcosta.kafka.core.TestProto.TestMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Pedro Costa
 * <p>
 * Units for {@link StringMessageKey} class
 */
public class StringMessageKeyTest {

    // the default topic used for the tests
    private String defaultTestTopic = "ADMIN";

    @Test
    public void generateKey() throws Exception {
        // Prepare
        final String messageName = TestMessage.getDefaultInstance().getClass().getName();
        final StringMessageKey stringMessageKey = new StringMessageKey(defaultTestTopic, messageName);

        //Call
        final String generatedKey = stringMessageKey.generateKey();

        //Assert
        assertEquals(stringMessageKey.getMessageType(), messageName);
        assertEquals(stringMessageKey.getSrcTopic(), defaultTestTopic);
        assertEquals(generatedKey, defaultTestTopic + "|" + messageName);
        assertNotNull(Class.forName(generatedKey.split("\\|")[1]));
    }

    @Test
    public void deserializeKey() {
        // Prepare
        final String messageName = TestMessage.getDefaultInstance().getClass().getName();
        final StringMessageKey<String> stringMessageKey = new StringMessageKey<>(defaultTestTopic + "|" + messageName);

        //Call
        stringMessageKey.deserializeKey(defaultTestTopic + "|" + messageName);

        //Assert
        assertEquals(stringMessageKey.getMessageType(), messageName);
        assertEquals(stringMessageKey.getSrcTopic(), defaultTestTopic);
    }

    @Test
    @SuppressWarnings("unused")
    public void deserializeKey_strangeTopic() {
        // Call
        final StringMessageKey<String> stringMessageKey = new StringMessageKey<>("798Topic//&$|invalidKey");
    }

    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings("unused")
    public void deserializeKey_invalidKey() throws Exception {
        // Call
        StringMessageKey<String> stringMessageKey = new StringMessageKey<>("invalidKey");
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unused")
    public void deserializeKey_nullKey() throws Exception {
        // Call
        final StringMessageKey<String> stringMessageKey = new StringMessageKey<>(null);
    }

}