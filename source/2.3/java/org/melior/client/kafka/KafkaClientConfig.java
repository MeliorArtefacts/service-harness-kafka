/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.client.kafka;
import org.melior.client.core.ClientConfig;
import org.springframework.core.io.Resource;

/**
 * Configuration parameters for a {@code KafkaClient}, with defaults.
 * @author Melior
 * @since 2.3
 */
public class KafkaClientConfig extends ClientConfig {

    private String serviceName = "kafka";

    private String jaas = "useTicketCache=false";

    private Resource kerberosConfig;

    private Resource keytab;

    private String topic;

    /**
     * Constructor.
     */
    protected KafkaClientConfig() {

        super();
    }

    /**
     * Configure client.
     * @param clientConfig The new client configuration parameters
     * @return The client configuration parameters
     */
    public KafkaClientConfig configure(
        final KafkaClientConfig clientConfig) {
        super.configure(clientConfig);
        this.serviceName = clientConfig.serviceName;
        this.jaas = clientConfig.jaas;
        this.kerberosConfig = clientConfig.kerberosConfig;
        this.keytab = clientConfig.keytab;
        this.topic = clientConfig.topic;

        return this;
    }

    /**
     * Get service name.
     * @return The service name
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Set service name.
     * @param serviceName The service name
     */
    public void setServiceName(
        final String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * Get JAAS configuration.
     * @return The JAAS configuration
     */
    public String getJaas(){
        return jaas;
    }

    /**
     * Set JAAS configuration.
     * @param jaas The JAAS configuration
     */
    public void setJaas(
        final String jaas) {
        this.jaas = jaas;
    }

    /**
     * Get Kerberos configuration path.
     * @return The Kerberos configuration path
     */
    public Resource getKerberosConfig() {
        return kerberosConfig;
    }

    /**
     * Set Kerberos configuration path.
     * @param kerberosConfig The Kerberos configuration path
     */
    public void setKerberosConfig(Resource kerberosConfig) {
        this.kerberosConfig = kerberosConfig;
    }

    /**
     * Get keytab path.
     * @return The keytab path
     */
    public Resource getKeytab() {
        return keytab;
    }

    /**
     * Set keytab path.
     * @param keytab The keytab path
     */
    public void setKeytab(
        final Resource keytab) {
        this.keytab = keytab;
    }

    /**
     * Get topic.
     * @return The topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Set topic.
     * @param topic The topic
     */
    public void setTopic(
        final String topic) {
        this.topic = topic;
    }

}
