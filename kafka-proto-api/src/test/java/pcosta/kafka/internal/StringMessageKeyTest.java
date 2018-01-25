package pcosta.kafka.internal;

import org.junit.Test;
import pcosta.kafka.core.TestProto.TestMessage;

import static org.junit.Assert.*;

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
        assertEquals(stringMessageKey.getKey(), defaultTestTopic + "|" + messageName);
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
        assertEquals(stringMessageKey.getKey(), defaultTestTopic + "|" + messageName);
        assertEquals(stringMessageKey.getMessageType(), messageName);
        assertEquals(stringMessageKey.getSrcTopic(), defaultTestTopic);
    }

    @Test
    @SuppressWarnings("unused")
    public void deserializeKey_strangeTopic() {
        // Prepare
        final String strangeKey = "798Topic//&$|StrangeKey";
        final StringMessageKey<String> stringMessageKey = new StringMessageKey<>(strangeKey);

        //Call
        stringMessageKey.deserializeKey(strangeKey);

        //Assert
        assertEquals(stringMessageKey.getKey(), strangeKey);
        assertEquals(stringMessageKey.getMessageType(), "StrangeKey");
        assertEquals(stringMessageKey.getSrcTopic(), "798Topic//&$");
    }

    @Test
    public void deserializeKey_invalidKey() {
        // Prepare
        final String unknownKey = "UnknownKey";
        final StringMessageKey<String> stringMessageKey = new StringMessageKey<>(unknownKey);

        //Call
        stringMessageKey.deserializeKey(unknownKey);

        //Assert
        assertEquals(stringMessageKey.getKey(), unknownKey);
        assertNull(stringMessageKey.getMessageType());
        assertNull(stringMessageKey.getSrcTopic());
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unused")
    public void deserializeKey_nullKey() {
        // Call
        final StringMessageKey<String> stringMessageKey = new StringMessageKey<>(null);
    }

}