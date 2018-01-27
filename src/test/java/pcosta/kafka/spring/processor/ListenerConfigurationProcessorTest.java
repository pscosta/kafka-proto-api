package pcosta.kafka.spring.processor;

import com.google.common.truth.Truth;
import org.junit.Test;
import pcosta.kafka.spring.ReceiverConfigurationProvider;
import pcosta.kafka.api.annotation.EnableListenerConfiguration;

import static com.google.testing.compile.JavaFileObjects.forResource;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

/**
 * @author Pedro Costa
 * <p>
 * Tests the {@link ListenerConfigurationProcessor} behaviour
 */
public class ListenerConfigurationProcessorTest {

    @Test
    public void test_success() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/EnableConfigListener_Ok.java")))
                .processedWith(new ListenerConfigurationProcessor())
                .compilesWithoutError();
    }

    @Test
    public void test_annotationOnInterface() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/EnableConfigListener_Method.java")))
                .processedWith(new ListenerConfigurationProcessor())
                .failsToCompile()
                .withErrorContaining(String.format(ListenerConfigurationProcessor.ONLY_CLASSES_CAN_BE_ANNOTATED,
                        EnableListenerConfiguration.class.getSimpleName()));
    }

    @Test
    public void test_withoutInterface() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/EnableConfigListener_Without_Interface.java")))
                .processedWith(new ListenerConfigurationProcessor())
                .failsToCompile()
                .withErrorContaining(String.format(ListenerConfigurationProcessor.CLASSES_ANNOTATED_IMPLEMENT_INTERFACE,
                        EnableListenerConfiguration.class.getSimpleName(), ReceiverConfigurationProvider.class.getSimpleName()));
    }

}
