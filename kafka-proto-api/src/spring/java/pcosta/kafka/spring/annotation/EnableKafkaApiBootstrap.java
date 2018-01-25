package pcosta.kafka.spring.annotation;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import pcosta.kafka.api.MessageListener;
import pcosta.kafka.spring.KafkaBeanFactory;

import java.lang.annotation.*;

/**
 * Enables default bootstrap of all defined {@link MessageListener}s from the Spring context annotated with
 * {@link MessagingListener}.
 * <p>
 * This annotation shall only be used at the most one time in all of the {@link Configuration} classes.
 *
 * @author Pedro Costa
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(KafkaBeanFactory.class)
public @interface EnableKafkaApiBootstrap {

    /**
     * Sets to whether or not the bootstrapping of the messaging API listeners shall be automatic or done manually.
     * By default this is set to {@code true}, such that the bootstrap is done whenever the spring context is firstly
     * initialized.
     *
     * @return {@code true} if the listeners bootstrap is to be done automatically, {@code false} otherwise.
     */
    boolean autoRegisterListeners() default true;
}
