package processor;

import pcosta.kafka.api.PlatformErrorListener;
import pcosta.kafka.spring.annotation.ErrorListener;

/**
 * @author Pedro Costa
 */
@ErrorListener
public interface ErrorListener_At_Interface extends PlatformErrorListener {
}
