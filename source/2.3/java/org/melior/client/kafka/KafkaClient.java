/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.client.kafka;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringSerializer;
import org.melior.client.exception.RemotingException;
import org.melior.context.service.ServiceContext;
import org.melior.context.transaction.TransactionContext;
import org.melior.logging.core.Logger;
import org.melior.logging.core.LoggerFactory;
import org.melior.logging.core.StreamSink;
import org.melior.service.exception.ExceptionType;
import org.melior.util.collection.NoNullsHashMap;
import org.melior.util.object.ObjectUtil;
import org.melior.util.string.StringUtil;
import org.melior.util.time.Timer;
import org.springframework.boot.logging.LogLevel;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.security.auth.module.Krb5LoginModule;

/**
 * Implements an easy to use, auto-configuring Kafka client with connection
 * pooling, configurable backoff strategy and automatic object mapping.
 * <p>
 * The client writes timing details to the logs while dispatching Kafka messages
 * to the Kafka server.  The client automatically converts any exception that
 * occurs during communication with the Kafka server into a standard
 * {@code RemotingException}.
 * @author Melior
 * @since 2.3
 */
@SuppressWarnings("restriction")
public class KafkaClient extends KafkaClientConfig {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private boolean async;

    private boolean kerberos;

    private boolean ssl;

    private ObjectMapper objectMapper;

    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Constructor.
     * @param async The asynchronous transport indicator
     * @param kerberos The Kerberos indicator
     * @param ssl The SSL indicator
     */
    KafkaClient(
        final boolean async,
        final boolean kerberos,
        final boolean ssl) {

        super();

        this.async = async;

        this.kerberos = kerberos;

        this.ssl = ssl;
    }

    /**
     * Configure client.
     * @param clientConfig The new client configuration parameters
     * @return The Kafka client
     */
    public KafkaClient configure(
        final KafkaClientConfig clientConfig) {
        super.configure(clientConfig);

        return this;
    }

    /**
     * Initialize client.
     * @throws RemotingException if unable to initialize the client
     */
    private void initialize() throws RemotingException {

        String protocol;
        String moduleName;
        java.util.Map<String, Object> properties;
        ProducerFactory<String, String> producerFactory;

        if (kafkaTemplate != null) {
            return;
        }

        if (StringUtils.hasLength(getUrl()) == false) {
            throw new RemotingException(ExceptionType.LOCAL_APPLICATION, "URL must be configured.");
        }

        if ((kerberos == true) && (getKerberosConfig() == null)) {
            throw new RemotingException(ExceptionType.LOCAL_APPLICATION, "Kerberos configuration must be configured.");
        }

        if ((kerberos == true) && (getKerberosConfig().isFile() == false)) {
            throw new RemotingException(ExceptionType.LOCAL_APPLICATION, "Kerberos configuration not found.");
        }

        if (StringUtils.hasLength(getUsername()) == false) {
            throw new RemotingException(ExceptionType.LOCAL_APPLICATION, "User name must be configured.");
        }

        if ((getKeytab() == null) && (StringUtils.hasLength(getPassword()) == false)) {
            throw new RemotingException(ExceptionType.LOCAL_APPLICATION, "Keytab or password must be configured.");
        }

        if ((getKeytab() != null) && (getKeytab().isFile() == false)) {
            throw new RemotingException(ExceptionType.LOCAL_APPLICATION, "Keytab not found.");
        }

        if (StringUtils.hasLength(getTopic()) == false) {
            throw new RemotingException(ExceptionType.LOCAL_APPLICATION, "Topic must be configured.");
        }

        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try {

            if (kerberos == true) {
                System.setProperty("java.security.krb5.conf", getKerberosConfig().getFile().getAbsolutePath());
            }

            protocol = ((kerberos == false) ? "" : "SASL_") + ((ssl == false) ? "PLAINTEXT" : "SSL");

            moduleName = (kerberos == false) ? PlainLoginModule.class.getName() : Krb5LoginModule.class.getName();

            properties = new NoNullsHashMap<String, Object>();
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, StringUtil.replaceAll(getUrl(), "tcp://", ""));
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, ServiceContext.getServiceName());
            properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, getRequestTimeout());
            properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, getRequestTimeout());
            properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, getRequestTimeout());
            properties.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, getBackoffPeriod());
            properties.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, getBackoffLimit());
            properties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, getInactivityTimeout());
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
            properties.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, (kerberos == false) ? null : getServiceName());
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig(moduleName));

            if (ssl == true) {

                properties.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, SSLEngineFactory.class);
                properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, getKeyStoreType());
                properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, (getKeyStore() == null) ? null : getKeyStore().getURL().toString());
                properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, getKeyStorePassword());
                properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, ObjectUtil.coalesce(getKeyPassword(), getKeyStorePassword()));
                properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, getTrustStoreType());
                properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, (getTrustStore() == null) ? null : getTrustStore().getURL().toString());
                properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, getTrustStorePassword());
                properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            }

            if (logger.isTraceEnabled() == true) {

                System.setProperty("sun.security.krb5.debug", "true");
                System.setOut(StreamSink.of(logger, "krb5debug", LogLevel.TRACE).getStream());
                System.setErr(StreamSink.of(logger, "krb5debug", LogLevel.ERROR).getStream());
            }

        }
        catch (Exception exception) {
            throw new RemotingException("Failed to parse producer configuration: " + exception.getMessage(), exception);
        }

        producerFactory = new DefaultKafkaProducerFactory<String, String>(properties);

        kafkaTemplate = new KafkaTemplate<String, String>(producerFactory);
        kafkaTemplate.setDefaultTopic(getTopic());
    }

    /**
     * Send message.
     * @param <Rq> The request type
     * @param message The message object
     * @throws RemotingException if unable to send the message
     */
    public <Rq> void send(
        final Rq message) throws RemotingException {

        String methodName = "send";
        String payload;
        TransactionContext transactionContext;
        Timer timer;
        ListenableFuture<SendResult<String, String>> sendResult;
        long duration;

        initialize();

        try {

            payload = (message instanceof String) ? (String) message : objectMapper.writeValueAsString(message);
        }
        catch (Exception exception) {
            throw new RemotingException(ExceptionType.LOCAL_APPLICATION, "Failed to serialize message: " + exception.getMessage(), exception);
        }

        if (payload != null) {
            logger.debug(methodName, "message = ", payload);
        }

        transactionContext = TransactionContext.get();

        timer = Timer.ofNanos().start();

        try {

            sendResult = kafkaTemplate.send(ProducerRecordFactory.create(getTopic(), payload, transactionContext.getTransactionId()));

            if (async == false) {

                try {

                    sendResult.get();
                }
                catch (InterruptedException exception) {
                    throw new KafkaException("Thread has been interrupted.", exception);
                }
                catch (ExecutionException exception) {
                    throw exception.getCause();
                }

            }

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Message sent successfully.  Duration = ", duration, " ms.");
        }
        catch (KafkaException exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Message send failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, translate(exception), exception);
        }
        catch (org.apache.kafka.common.KafkaException exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Message send failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, translate(exception), exception);
        }
        catch (Throwable exception) {
            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, "Failed to send message: " + exception.getMessage(), exception);
        }

    }

    /**
     * Build JAAS configuration.
     * @param moduleName The module name
     * @return The JAAS configuration
     * @throws IOException if a keytab is provided but cannot be accessed
     */
    private String buildJaasConfig(
        final String moduleName) throws IOException {

        String methodName = "buildJaasConfig";
        String jaasConfig;

        jaasConfig = moduleName + " required doNotPrompt=true"
            + ((kerberos == false) ? "" : " serviceName=\"" + getServiceName() + "\"") + ((getKeytab() != null)
            ? " principal=\"" + getUsername() + "\" useKeyTab=true keyTab=\"" + getKeytab().getFile().getAbsolutePath().replace('\\', '/') + "\" "
            : " username=\"" + getUsername() + "\" password=\"" + getPassword() + "\" ") + getJaas() + " client=true;";

        if (logger.isTraceEnabled() == true) {
            logger.debug(methodName, "jaasConfig = ", jaasConfig);
        }

        return jaasConfig;
    }

    /**
     * Translate exception message.
     * @param exception The exception
     * @return The translated exception message
     */
    private String translate(
        Throwable exception) {

        String message = exception.getMessage();

        while ((kerberos == true) && (exception != null)) {

            if (StringUtil.contains(exception.getMessage(), "Unable to obtain password from user") == true) {

                message = "Unable to authenticate user.  User domain does not match realm (case sensitive), "
                    + "or keytab does not contain valid key for user.";

                break;
            }

            exception = exception.getCause();
        }

        return message;
    }

}
