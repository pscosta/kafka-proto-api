package pcosta.kafka.spring.annotation;

import pcosta.kafka.api.PlatformErrorListener;
import com.google.common.truth.Truth;
import org.junit.Test;

import static com.google.testing.compile.JavaFileObjects.forResource;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

/**
 * Unit tests for the {@link ErrorListenerProcessor} class.
 *
 * @author Pedro Costa
 */
public class ErrorListenerProcessorTest {

    @Test
    public void test_success() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/ErrorListener_Ok.java")))
                .processedWith(new ErrorListenerProcessor())
                .compilesWithoutError();
    }

    @Test
    public void test_annotationOnMethod() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/ErrorListener_Method_Ok.java")))
                .processedWith(new ErrorListenerProcessor())
                .compilesWithoutError();
    }

    @Test
    public void test_annotationOnInterface() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/ErrorListener_At_Interface.java")))
                .processedWith(new ErrorListenerProcessor())
                .failsToCompile()
                .withErrorContaining(String.format(ErrorListenerProcessor.ONLY_CLASSES_AND_METHODS_CAN_BE_ANNOTATED, ErrorListener.class.getSimpleName()));
    }

    @Test
    public void test_withoutInterface() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/ErrorListener_Without_Interface.java")))
                .processedWith(new ErrorListenerProcessor())
                .failsToCompile()
                .withErrorContaining(String.format(ErrorListenerProcessor.CLASSES_ANNOTATED_IMPLEMENT_INTERFACE,
                        ErrorListener.class.getSimpleName(), PlatformErrorListener.class.getSimpleName()));
    }

    @Test
    public void test_multipleAnnotations() {
        Truth.ASSERT.about(javaSource())
                .that(forResource(getClass().getResource("/processor/ErrorListener_Multiple_Annotated_Listeners.java")))
                .processedWith(new ErrorListenerProcessor())
                .failsToCompile()
                .withErrorContaining(String.format(ErrorListenerProcessor.ONLY_ONE_CLASS_ANNOTATED, ErrorListener.class.getSimpleName()));
    }

}
