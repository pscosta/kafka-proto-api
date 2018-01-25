package pcosta.kafka.spring.annotation;

import pcosta.kafka.api.MessageListener;
import com.google.protobuf.Message;
import com.sun.tools.javac.code.Symbol;
import com.sun.tools.javac.code.Type;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import java.util.*;

/**
 * The compile time processor for the {@link MessagingListener} annotation.
 *
 * @author Pedro Costa
 */
public class MessagingListenerProcessor extends AbstractProcessor {

    // error messages
    static final String ONLY_CLASSES_CAN_BE_ANNOTATED = "only classes can be annotated with @%s";
    static final String CLASSES_ANNOTATED_MUST_IMPLEMENT = "classes annotated with @%s must implement %s";
    static final String UNABLE_TO_DETERMINE_MESSAGE_TYPE = "unable to determine the type of message being listened to";
    static final String MESSAGE_TYPE_IS_NOT_COMPATIBLE = "%s isn't compatible with %s";
    static final String REPEATED_TOPICS = "repeated topics are unsupported";

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(MessagingListener.class.getCanonicalName());
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

        // get the message listener interface type
        final TypeMirror messageListenerType = processingEnv
                .getTypeUtils()
                .erasure(processingEnv.getElementUtils().getTypeElement(MessageListener.class.getCanonicalName()).asType());

        // get the generated message type
        final TypeMirror MessageType = processingEnv
                .getElementUtils()
                .getTypeElement(Message.class.getCanonicalName())
                .asType();

        for (final Element annotatedElement : roundEnv.getElementsAnnotatedWith(MessagingListener.class)) {
            // check if a class has been annotated with @MessagingListener
            if (annotatedElement.getKind() != ElementKind.CLASS) {
                return error(annotatedElement, ONLY_CLASSES_CAN_BE_ANNOTATED, MessagingListener.class.getSimpleName());
            }

            // check if there's the MessageListener interface implemented
            if (!processingEnv.getTypeUtils().isAssignable(processingEnv.getTypeUtils().erasure(annotatedElement.asType()), messageListenerType)) {
                return error(annotatedElement, CLASSES_ANNOTATED_MUST_IMPLEMENT, MessagingListener.class.getSimpleName(), MessageListener.class.getCanonicalName());
            }

            // get the annotation
            final MessagingListener annotation = annotatedElement.getAnnotation(MessagingListener.class);

            // validate the message type if the default is used, check if the message listener interface has indeed a generic type associated
            if (hasDefaultMessageType(annotation)) {
                final Type interfaceElement = getMessageListenerTypeFromInterfaces(annotatedElement);
                if (interfaceElement == null || interfaceElement.getTypeArguments().isEmpty()) {
                    return error(annotatedElement, UNABLE_TO_DETERMINE_MESSAGE_TYPE);
                }

                // extract the generic type
                final Type genericType = interfaceElement.getTypeArguments().iterator().next();
                if (!processingEnv.getTypeUtils().isAssignable(genericType, MessageType)) {
                    return error(annotatedElement, MESSAGE_TYPE_IS_NOT_COMPATIBLE, genericType.asElement().getSimpleName(), Message.class.getSimpleName());
                }
            }

            // validate the metadata, check if the class + field exists and are accessible
            if (!areTopicsValid(annotation.topic())) {
                return error(annotatedElement, REPEATED_TOPICS, annotation.topic());
            }
        }

        // ok!
        return false;
    }

    /**
     * Returns the message listener interface type from the annotated element or null if the element does not
     * implement the interface (directly or indirectly)
     *
     * @param annotatedElement the annotated element
     * @return the message listener interface
     */
    private Type getMessageListenerTypeFromInterfaces(final Element annotatedElement) {
        // get the message listener type
        final TypeMirror messageListenerType = processingEnv
                .getTypeUtils()
                .erasure(processingEnv.getElementUtils().getTypeElement(MessageListener.class.getCanonicalName()).asType());

        // check if the element implements MessageListener (directly or through an interface)
        if (processingEnv.getTypeUtils().isAssignable(annotatedElement.asType(), messageListenerType)) {
            // return the MessageListener interface
            return getMessageListenerType(annotatedElement, messageListenerType);
        }

        return null;
    }

    /**
     * Returns the message listener interface type from the annotated element or from any of its super classes
     *
     * @param element             the annotated element
     * @param messageListenerType the message listener type we are looking for
     * @return the message listener interface
     */
    private Type getMessageListenerType(final Element element, TypeMirror messageListenerType) {
        final List<Type> interfaces = ((Symbol.ClassSymbol) element).getInterfaces();

        for (Type type : interfaces) {
            final TypeMirror erasedType = processingEnv.getTypeUtils().erasure(type);
            if (processingEnv.getTypeUtils().isSameType(erasedType, messageListenerType)) {
                return type;
            }
        }

        List<? extends TypeMirror> typeMirrors = processingEnv.getTypeUtils().directSupertypes(element.asType());
        Symbol.ClassSymbol classSymbol = (Symbol.ClassSymbol) processingEnv.getTypeUtils().asElement(typeMirrors.iterator().next());
        return getMessageListenerType(classSymbol, messageListenerType);
    }

    /**
     * Checks the configuration to see if the default type was used
     *
     * @param annotation the annotation configuration
     * @return {@code true} if the default message type has been used,
     * {@code false} otherwise
     */
    private boolean hasDefaultMessageType(final MessagingListener annotation) {
        try {
            return annotation.message() == DEFAULT_MESSAGE_TYPE.class;
        } catch (final MirroredTypeException e) {
            return processingEnv.getTypeUtils().isSameType(e.getTypeMirror(), processingEnv
                    .getElementUtils()
                    .getTypeElement(DEFAULT_MESSAGE_TYPE.class.getCanonicalName())
                    .asType());
        }
    }

    /**
     * Validates if the received topics are known and not repeated
     *
     * @param topics the topics to be validates
     * @return {@code true} if the configuration is valid, {@code false} otherwise.
     */
    private boolean areTopicsValid(final String... topics) {
        return topics != null && new HashSet<>(Arrays.asList(topics)).size() == Arrays.asList(topics).size();
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
