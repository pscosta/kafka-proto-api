package pcosta.kafka.spring.annotation;

import pcosta.kafka.api.MessageFilter;
import pcosta.kafka.api.MessageListener;
import pcosta.kafka.spring.DefaultExtensionRegistrySupplier;
import pcosta.kafka.spring.ExtensionRegistrySupplier;
import com.google.protobuf.Message;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The messaging listener annotation, which shall defined a {@link MessageListener} for a specific type of message
 * being processed.
 *
 * @author costa
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface MessagingListener {

    /**
     * The actual class type of the message that is going to be received and processed.
     * If no type is specified, the bootstrap looks for the generic type defined when implementing the {@link MessagingListener} interface.
     *
     * @return the type of the message
     */
    Class<? extends Message> message() default DEFAULT_MESSAGE_TYPE.class;

    /**
     * The Topic where the expected messages will be retrieved from
     *
     * @return the message topic
     */
    String topic();

    /**
     * Defines the filters to be applied for this listener configuration
     *
     * @return the type of filters
     */
    Class<? extends MessageFilter>[] filters() default {};

    /**
     * Defines the Supplier for the {@link com.google.protobuf.ExtensionRegistry} used to parse extension fields on this listener's message type
     */
    Class<? extends ExtensionRegistrySupplier> extensionRegistry() default DefaultExtensionRegistrySupplier.class;

    /**
     * The default message partition for the configured {@code Topic}s
     *
     * @return the configured partition. Partition {@code 0} will be returned by default
     */
    int partition() default 0;
}
