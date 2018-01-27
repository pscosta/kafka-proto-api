package pcosta.kafka.api.annotation;

import pcosta.kafka.api.MessageListener;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The messaging listener annotation for the platform messages, which shall defined a {@link MessageListener}
 * for a specific platform message being processed.
 *
 * @author Pedro Costa
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.TYPE_USE})
public @interface ErrorListener {
    // noop
}
