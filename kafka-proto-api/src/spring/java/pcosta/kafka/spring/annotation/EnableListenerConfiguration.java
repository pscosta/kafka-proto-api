package pcosta.kafka.spring.annotation;

import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.MessageReceiverConfiguration;
import pcosta.kafka.spring.ReceiverConfigurationProvider;
import org.springframework.context.annotation.Configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Pedro Costa
 * <p>
 * Enables the {@link MessageListener} registration via the {@link MessageReceiverConfiguration} provided instance
 * <p>
 * All Annotated {@code EnableListenerConfiguration} types must implement the {@link ReceiverConfigurationProvider} interface
 * <p>
 * This annotation shall only be used at the most one time in all of the {@link Configuration} classes.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface EnableListenerConfiguration {
    // noop
}