package pcosta.kafka.spring.annotation;

import org.springframework.beans.factory.annotation.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

/**
 * @author Pedro Costa
 * <p>
 * Stereotype used to select a {@link pcosta.kafka.api.MessageProducer} for String type
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({METHOD, FIELD, PARAMETER,})
public @interface StringKafkaSender {
    // noop
}
