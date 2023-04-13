/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.client.kafka;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.utils.SecurityUtils;
import org.melior.client.core.ClientConfig;
import org.melior.client.ssl.ClientKeyStore;
import org.melior.client.ssl.ClientSSLContext;
import org.melior.util.object.ObjectUtil;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import java.security.KeyStore;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/**
 * Instantiates an {@code SSLEngine} for use in Kafka clients that establish secure
 * connections to Kafka servers.  The {@code SSLContext} from which the {@code SSLEngine}
 * is created can either be fully trusting or will require a concrete key store and/or
 * a concrete trust store that is referenced in the remoting client configuration.
 * @author Melior
 * @since 2.3
 */
public final class SSLEngineFactory implements SslEngineFactory {

    private Map<String, ?> configs;

    private String protocol;

    private String[] enabledCipherSuites;

    private String[] enabledProtocols;

    private SslClientAuth sslClientAuth;

    private KeyStore keyStore;

    private KeyStore trustStore;

    private SSLContext sslContext;

    /**
     * Create new SSLEngine to be used by client.
     * @param peerHost The peer host to use
     * @param peerPort The peer port to use
     * @param endpointIdentification The end-point identification algorithm
     * @return The SSLEngine 
     */
    public SSLEngine createClientSslEngine(
        final String peerHost,
        final int peerPort,
        final String endpointIdentification) {

        SSLEngine sslEngine;
        SSLParameters sslParameters;

        sslEngine = sslContext.createSSLEngine(peerHost, peerPort);

        if (enabledCipherSuites != null) sslEngine.setEnabledCipherSuites(enabledCipherSuites);

        if (enabledProtocols != null) sslEngine.setEnabledProtocols(enabledProtocols);

        sslEngine.setUseClientMode(true);

        sslParameters = sslEngine.getSSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm(endpointIdentification);
        sslEngine.setSSLParameters(sslParameters);

        return sslEngine;
    }

    /**
     * Create new SSLEngine to be used by server.
     * @param peerHost The peer host to use
     * @param peerPort The peer port to use
     * @return The SSLEngine 
     */
    public SSLEngine createServerSslEngine(
        final String peerHost,
        final int peerPort) {

        SSLEngine sslEngine;

        sslEngine = sslContext.createSSLEngine(peerHost, peerPort);

        if (enabledCipherSuites != null) sslEngine.setEnabledCipherSuites(enabledCipherSuites);

        if (enabledProtocols != null) sslEngine.setEnabledProtocols(enabledProtocols);

        sslEngine.setUseClientMode(false);

        switch (sslClientAuth) {
        case REQUIRED:
            sslEngine.setNeedClientAuth(true);
            break;
        case REQUESTED:
            sslEngine.setWantClientAuth(true);
            break;
        case NONE:
            break;
        }

        return sslEngine;
    }

    /**
     * Determine if SSLEngine needs to be rebuilt.
     * @param newConfigs The new configuration to use
     * @return true if the SSLEngine needs to be rebuilt, false otherwise
     */
    public boolean shouldBeRebuilt(
        final Map<String, Object> newConfigs) {
        return newConfigs.equals(configs) == false;
    }

    /**
     * Return names of configuration parameters that may be reconfigured.
     * @return The set of names
     */
    public Set<String> reconfigurableConfigs() {
        return SslConfigs.RECONFIGURABLE_CONFIGS;
    }

    /**
     * Returns the key store used by the SSL engine.
     * @return The key store
     */
    public KeyStore keystore() {
        return keyStore;
    }

    /**
     * Returns the trust store used by the SSL engine.
     * @return The trust store
     */
    public KeyStore truststore() {
        return trustStore;
    }

    /**
     * Configure the SSL engine factory.
     * @param configs The configuration parameters
     */
    @SuppressWarnings("unchecked")
    public void configure(
        final Map<String, ?> configs) {

        List<String> enabledCipherSuiteList;
        List<String> enabledProtocolList;
        ClientConfig clientConfig;

        this.configs = configs;

        SecurityUtils.addConfiguredSecurityProviders(configs);

        protocol = (String) configs.get(SslConfigs.SSL_PROTOCOL_CONFIG);

        enabledCipherSuiteList = (List<String>) configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        enabledCipherSuites = ((enabledCipherSuiteList != null) && (enabledCipherSuiteList.isEmpty() == false))
            ? enabledCipherSuiteList.toArray(new String[enabledCipherSuiteList.size()]) : null;

        enabledProtocolList = (List<String>) configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        enabledProtocols = ((enabledProtocolList != null) && (enabledProtocolList.isEmpty() == false))
            ? enabledProtocolList.toArray(new String[enabledProtocolList.size()]) : null;

        sslClientAuth = ObjectUtil.coalesce(SslClientAuth.forConfig((String) configs.get(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG)), SslClientAuth.NONE);

        clientConfig = new KafkaClientConfig();
        clientConfig.setKeyStore(getResource((String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)));
        clientConfig.setKeyStoreType((String) configs.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));
        clientConfig.setKeyStorePassword(getPassword((Password) configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)));
        clientConfig.setKeyPassword(getPassword((Password) configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG)));
         clientConfig.setTrustStore(getResource((String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)));
        clientConfig.setTrustStoreType((String) configs.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
        clientConfig.setTrustStorePassword(getPassword((Password) configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)));

        keyStore = (clientConfig.getKeyStore() == null) ? null : ClientKeyStore.ofKey(clientConfig);

        trustStore = (clientConfig.getTrustStore() == null) ? null : ClientKeyStore.ofTrust(clientConfig);

        sslContext = ((keyStore != null) || (trustStore != null))
            ? ClientSSLContext.ofKeyStore(protocol, clientConfig, keyStore, trustStore) : ClientSSLContext.ofLenient(protocol);
    }

    /**
     * Close SSL engine factory.
     */
    public void close() {
        sslContext = null;
    }

    /**
     * Convert specified location to a resource.  If the location is {@code null}
     * then the returned resource is {@code null}.
     * @param location The location
     * @return The resource
     */
    private Resource getResource(
        final String location) {
        return (location == null) ? null : new DefaultResourceLoader().getResource(location);
    }

    /**
     * Extract password from password wrapper.  If the wrapper is {@code null}
     * then the returned password is {@code null}.
     * @param password The password wrapper
     * @return The password
     */
    private String getPassword(
        final Password password) {
        return (password == null) ? null : password.value();
    }

}
