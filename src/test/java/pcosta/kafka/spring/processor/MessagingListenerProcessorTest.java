package pcosta.kafka.spring.processor;

import com.google.common.truth.Truth;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Parser;
import org.junit.Test;
import pcosta.kafka.api.MessageListener;
import pcosta.kafka.spring.annotation.MessagingListener;

import static com.google.testing.compile.JavaFileObjects.forResource;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for the {@link MessagingListenerProcessor} class.
 *
 * @author Pedro Costa
 */
public class MessagingListenerProcessorTest {

    @SuppressWarnings("unused")
    public static class TestHeaders {

        public static final String METADATA = "GoodTopic";
    }

    @SuppressWarnings({"unchecked", "unused"})
    public static abstract class TestMessage extends GeneratedMessage {

        public static final Parser<TestMessage> PARSER = mock(Parser.class);
    }

    @Test
    public void test_success() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/MessagingListener_Ok.java")))
                .processedWith(new MessagingListenerProcessor())
                .compilesWithoutError();
    }

    @Test
    public void test_generic_type() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/MessagingListener_GenericType.java")))
                .processedWith(new MessagingListenerProcessor())
                .compilesWithoutError();
    }

    @Test
    public void test_generic_type_v3() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/MessagingListener_GenericTypeV3.java")))
                .processedWith(new MessagingListenerProcessor())
                .compilesWithoutError();
    }

    @Test
    public void test_default_type() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/MessagingListener_DefaultType.java")))
                .processedWith(new MessagingListenerProcessor())
                .compilesWithoutError();
    }

    @Test
    public void test_default_type_message() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/MessagingListener_DefaultType_Message.java")))
                .processedWith(new MessagingListenerProcessor())
                .compilesWithoutError();
    }

    @Test
    public void test_kafkaMessage_type_message() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/MessagingListener_KafkaMessage.java")))
                .processedWith(new MessagingListenerProcessor())
                .compilesWithoutError();
    }

    @Test
    public void test_annotationOnInterface() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/MessagingListener_At_Interface.java")))
                .processedWith(new MessagingListenerProcessor())
                .failsToCompile()
                .withErrorContaining(String.format(MessagingListenerProcessor.ONLY_CLASSES_CAN_BE_ANNOTATED,
                        MessagingListener.class.getSimpleName()));
    }

    @Test
    public void test_annotationWithoutListenerInterface() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/MessagingListener_Without_Interface.java")))
                .processedWith(new MessagingListenerProcessor())
                .failsToCompile()
                .withErrorContaining(String.format(MessagingListenerProcessor.CLASSES_ANNOTATED_MUST_IMPLEMENT,
                        MessagingListener.class.getSimpleName(), MessageListener.class.getCanonicalName()));
    }

    @Test
    public void test_annotationWithoutMessageTypeDefined() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/MessagingListener_Without_MessageType.java")))
                .processedWith(new MessagingListenerProcessor())
                .failsToCompile()
                .withErrorContaining("processor.MessagingListener_Without_MessageType is not abstract and does not override abstract method onMessage");
    }

    @Test
    public void test_annotationWithoutIncompatibleMessageType() {
        Truth.ASSERT
                .about(javaSource())
                .that(forResource(getClass().getResource("/processor/MessagingListener_MessageType_Incompatible.java")))
                .processedWith(new MessagingListenerProcessor())
                .failsToCompile()
                .withErrorContaining("type argument java.lang.String is not within bounds");
    }

    @Test
    public void test_annotationWithAbstractMessageListener() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/MessagingListener_Extends_AbstractMessageListener.java")))
                .processedWith(new MessagingListenerProcessor())
                .compilesWithoutError();
    }

    @Test
    public void test_annotationWithInvalidMetadata() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/MessagingListener_Empty_Topic.java")))
                .processedWith(new MessagingListenerProcessor())
                .compilesWithoutError();
    }
}
