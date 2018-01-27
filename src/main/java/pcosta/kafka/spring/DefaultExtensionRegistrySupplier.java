package pcosta.kafka.spring;

import com.google.protobuf.ExtensionRegistry;

/**
 * @author Pedro Costa
 * <p>
 * The default Supplier for {@link com.google.protobuf.ExtensionRegistry}s that supplies a default empty ExtensionRegistry
 */
public class DefaultExtensionRegistrySupplier implements ExtensionRegistrySupplier {

    @Override
    public ExtensionRegistry get() {
        return ExtensionRegistry.getEmptyRegistry();
    }
}
