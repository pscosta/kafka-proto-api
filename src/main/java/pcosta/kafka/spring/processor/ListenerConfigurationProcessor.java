package pcosta.kafka.spring.processor;

import pcosta.kafka.api.PlatformErrorListener;
import pcosta.kafka.spring.ReceiverConfigurationProvider;
import pcosta.kafka.api.annotation.EnableListenerConfiguration;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import java.util.Collections;
import java.util.Set;

/**
 * @author Pedro Costa
 * <p>
 * The compile time processor for the {@link EnableListenerConfiguration} annotation.
 */
public class ListenerConfigurationProcessor extends AbstractProcessor {
    // error messages
    static final String ONLY_CLASSES_CAN_BE_ANNOTATED = "only classes and methods can be annotated with @%s";
    static final String ONLY_ONE_CLASS_ANNOTATED = "at the most there must be only one class annotated with @%s";
    static final String CLASSES_ANNOTATED_IMPLEMENT_INTERFACE = "classes annotated with @%s must implement %s";

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(EnableListenerConfiguration.class.getCanonicalName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv) {
        if (roundEnv.processingOver()) {
            return false;
        }

        // get the annotations
        final Set<? extends Element> listenerConfigAnnotations = roundEnv.getElementsAnnotatedWith(EnableListenerConfiguration.class);

        // at the most there must be one annotation
        if (listenerConfigAnnotations.size() > 1) {
            return error(null, ONLY_ONE_CLASS_ANNOTATED, EnableListenerConfiguration.class.getSimpleName());
        }

        final Element annotatedElement = listenerConfigAnnotations.iterator().next();
        if (annotatedElement.getKind() != ElementKind.CLASS) {
            return error(annotatedElement, ONLY_CLASSES_CAN_BE_ANNOTATED, EnableListenerConfiguration.class.getSimpleName(), PlatformErrorListener.class.getSimpleName());
        }
        // check if there's the ReceiverConfigurationProvider interface implemented
        final TypeMirror receiverConfigurationProviderType = processingEnv
                .getElementUtils()
                .getTypeElement(ReceiverConfigurationProvider.class.getCanonicalName())
                .asType();

        if (!processingEnv.getTypeUtils().isAssignable(annotatedElement.asType(), receiverConfigurationProviderType)) {
            return error(annotatedElement, CLASSES_ANNOTATED_IMPLEMENT_INTERFACE,
                    EnableListenerConfiguration.class.getSimpleName(), ReceiverConfigurationProvider.class.getSimpleName());
        }
        // ok!
        return false;
    }

    /**
     * Emits the specified error message and return the exit code
     *
     * @param e    the element that caused the error
     * @param msg  the error message
     * @param args the message arguments, if any
     * @return the error code
     */
    private boolean error(final Element e, final String msg, final Object... args) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, String.format(msg, args), e);
        return true;
    }
}
