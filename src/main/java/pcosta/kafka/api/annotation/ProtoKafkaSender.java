package pcosta.kafka.api.annotation;

import org.springframework.beans.factory.annotation.Qualifier;
import pcosta.kafka.api.MessageProducer;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

/**
 * @author Pedro Costa
 * <p>
 * Stereotype used to select a {@link MessageProducer} for Protobuf type
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({METHOD, FIELD, PARAMETER,})
public @interface ProtoKafkaSender {
    // noop
}
