package pcosta.kafka.spring;

import com.google.protobuf.ExtensionRegistry;

import java.util.function.Supplier;

/**
 * @author Pedro Costa
 * <p>
 * Supplier {@link com.google.protobuf.ExtensionRegistry}s used to parse extension fields on protobuf message types.
 * <p>
 * When parsing a protobuf message that might have extensions, an ExtensionRegistry must be provided,
 * Otherwise, those extensions will just be treated like unknown fields.
 */
public interface ExtensionRegistrySupplier extends Supplier<ExtensionRegistry> {

    /**
     * The Extension Registry for a given protobuf type
     *
     * @return the supplied message registry
     */
    ExtensionRegistry get();

}
