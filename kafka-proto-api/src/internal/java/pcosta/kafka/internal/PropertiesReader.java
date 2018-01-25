package pcosta.kafka.internal;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * @author Pedro Costa
 * <p>
 * Defines the kafka sender and receiver properties and loads the file overriding values, if defined
 */
final class PropertiesReader {

    private static final Logger log = LoggerFactory.getLogger(PropertiesReader.class);

    private final Properties fileProperties = new Properties();

    // the kafka configuration file
    private static final String CONFIGURATION_FILE = "kafka.properties";
    // system property set by installer / program args
    public static final String CONFIGURATION_FILE_DIR = "config.file.dir";
    // the default kafka broker location
    private static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:12100";
    // system property set by test infrastructure
    private static final String SPRING_EMBEDDED_KAFKA_BROKERS = "spring.embedded.kafka.brokers";
    // kafka receiver default auto-commit configuration
    private static final boolean DEFAULT_AUTO_COMMIT = true;

    // the holder class
    private static final class Holder {

        private static final PropertiesReader reader = new PropertiesReader();
    }

    /**
     * Default constructor
     */
    private PropertiesReader() {
        readConfiguration();
    }

    /**
     * Returns the instance of the reader object
     *
     * @return the reader instance
     */
    static PropertiesReader getInstance() {
        return Holder.reader;
    }

    /**
     * Returns the current defined properties
     *
     * @return the defined properties
     */
    Properties getProperties() {
        return fileProperties;
    }

    /**
     * Creates the Consumer properties and loads the properties defined at configuration file
     *
     * @return the Consumer properties
     */
    Map<String, Object> loadConsumerProps() {
        Map<String, Object> properties = new HashMap<>();

        // load SPRING_EMBEDDED_KAFKA_BROKERS prop from file; if not set, load from System
        final String fileProperty = fileProperties.getProperty(SPRING_EMBEDDED_KAFKA_BROKERS, DEFAULT_BOOTSTRAP_SERVER);
        final String bootstrapServer = System.getProperty(SPRING_EMBEDDED_KAFKA_BROKERS, fileProperty);

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DEFAULT_AUTO_COMMIT);
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 50 * 1024 * 1024); //50 MByte lol
        properties.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 3000);
        properties.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        //Load the properties defined at kafka properties file
        fileProperties.entrySet().stream()
                .filter(prop -> prop.getKey().toString().startsWith("consumer."))
                .forEach(prop -> Optional.ofNullable(
                        properties.put(String.valueOf(prop.getKey()).replaceFirst("consumer.", ""), prop.getValue()))
                        .ifPresent(old -> log.info("Overriding consumer property: {} with {}", old, prop.getValue())));

        return properties;
    }

    /**
     * Creates the Sender properties and loads the properties defined at configuration file
     *
     * @return the Sender properties
     */
    Map<String, Object> loadSenderProps() {
        Map<String, Object> properties = new HashMap<>();

        // load SPRING_EMBEDDED_KAFKA_BROKERS prop from file; if not set, load from System
        final String fileProperty = fileProperties.getProperty(SPRING_EMBEDDED_KAFKA_BROKERS, DEFAULT_BOOTSTRAP_SERVER);
        final String bootstrapServer = System.getProperty(SPRING_EMBEDDED_KAFKA_BROKERS, fileProperty);

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 3000);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 3000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000); //how long producer.send() and producer.partitionsFor() will block
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 50 * 1024 * 1024); //50 MByte lol
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //Load the properties defined at kafka properties file
        fileProperties.entrySet().stream()
                .filter(prop -> prop.getKey().toString().startsWith("sender."))
                .forEach(prop -> Optional.ofNullable(
                        properties.put(String.valueOf(prop.getKey()).replaceFirst("sender.", ""), prop.getValue()))
                        .ifPresent(old -> log.info("Overriding consumer property: {} with {}", old, prop.getValue())));

        return properties;
    }

    /**
     * Reads the configuration file
     */
    private void readConfiguration() {
        // read the properties
        try (final InputStream resourceStream = getConfigurationStream()) {
            fileProperties.load(resourceStream);
        } catch (final IOException e) {
            log.debug("File {} does not exist. No custom properties will be set.", CONFIGURATION_FILE);
        }
    }

    /**
     * Returns the input stream for the configuration file
     *
     * @return the input stream
     */
    private InputStream getConfigurationStream() throws FileNotFoundException {
        String path = System.getProperty(CONFIGURATION_FILE_DIR, "");
        return new FileInputStream(path + (path.endsWith("/") ? "" : "/") + CONFIGURATION_FILE);
    }
}
