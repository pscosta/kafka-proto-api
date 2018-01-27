package pcosta.kafka.api.annotation;

import com.google.protobuf.AbstractMessage;

/**
 * the default message type, denoting that no message type has been specified
 *
 * @author Pedro Costa
 */
public abstract class DEFAULT_MESSAGE_TYPE extends AbstractMessage {

    // private constructor, no initializations shall be made possible
    private DEFAULT_MESSAGE_TYPE() {
    }
}
