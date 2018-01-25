package pcosta.kafka.internal;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Internal;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import pcosta.kafka.message.KafkaMessageProto.KafkaMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Pedro Costa
 * <p>
 * The deserializer for protobuf types
 */
@SuppressWarnings({"unchecked", "unused"})
public class ProtobufDeserializer<M extends Message> {

    // protobuf default instances cache
    private Map<String, Message> typesCache = new ConcurrentHashMap<>();

    /**
     * Extracts and deserializes the {@link Message} concrete payload protobuf object.
     * {@link com.google.protobuf.Any#unpack(Class)} cannot be used as it doesn't provide native support for Extensions
     *
     * @param message  the {@link KafkaMessage} with the data to be extracted
     * @param registry the extension registry used to parse extension fields
     * @return the deserialized proto KafkaMessage payload
     */
    public M parseFromV3(KafkaMessage message, ExtensionRegistry registry) {
        try {
            // checking the type cache to avoid unnecessary reflective calls
            Message payloadInstance = typesCache.get(message.getPayloadClass());
            if (payloadInstance == null) {
                payloadInstance = Internal.getDefaultInstance((Class<M>) Class.forName(message.getPayloadClass()));
                typesCache.put(message.getPayloadClass(), payloadInstance);
            }
            // deserialize and parse the KafkaMessage payload (defined as Any proto type)
            return (M) payloadInstance.getParserForType().parseFrom(message.getPayload().getValue(), registry);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage() + " for PayloadClass: " + message.getPayloadClass(), e);
        }
    }

    /**
     * Parses the specified byte array and deserializes it into its concrete protobuf object
     *
     * @param type  the target protobuf type
     * @param bytes the raw byte array where the data is to be extracted
     * @return the parsed protobuf message
     */
    public M parseFrom(Class<M> type, byte[] bytes) {
        try {
            Message msg = (Message) type.getMethod("getDefaultInstance").invoke(null);
            return (M) msg.newBuilderForType().mergeFrom(bytes).build();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * Parses the specified byte array and deserializes it into its concrete protobuf object,
     * described by the given fully qualified {@code typeName}
     *
     * @param typeName the target protobuf type fully qualified name
     * @param bytes    the raw byte array where the data is to be extracted
     * @return the parsed protobuf message
     */
    public M parseFrom(String typeName, byte[] bytes) {
        try {
            Class<?> type = Class.forName(typeName);
            Message msg = (Message) type.getMethod("getDefaultInstance").invoke(null);
            return (M) msg.newBuilderForType().mergeFrom(bytes).build();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * Parses the specified byte array and deserializes it into its concrete protobuf object
     *
     * @param type     the target protobuf type
     * @param bytes    the raw byte array where the data is to be extracted
     * @param registry the extension registry used to parse extension fields
     * @return the parsed protobuf message
     */
    public M parseFrom(Class<M> type, byte[] bytes, ExtensionRegistry registry) {
        try {
            final Parser<M> parser = (Parser<M>) (type).getField("PARSER").get(null);
            return parser.parseFrom(bytes, registry);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * Util method that provides an {@link Message} default instance
     *
     * @return the {@link Message} default instance
     */
    public M getKafkaTypeDefaultInstance() {
        try {
            return (M) KafkaMessage.getDefaultInstance();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
