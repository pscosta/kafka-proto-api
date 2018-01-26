package pcosta.kafka.internal;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author Pedro Costa
 * <p>
 * ByteArray Serializer for protobuf types
 */
public class ProtobufSerializer<M extends Message> implements Serializer<M> {

    @Override
    public void configure(Map configs, boolean isKey) {
        //noop
    }

    /**
     * Receives the protobuf message and converts it into a {@code byte[]}
     *
     * @param topic the app topic, needed to respect the overridden interface
     * @param data  the protobuf message to be serialized
     * @return the proto message serialized into a byte[]
     */
    @Override
    public byte[] serialize(String topic, M data) {
        return data.toByteArray();
    }

    @Override
    public void close() {
        //noop
    }
}
