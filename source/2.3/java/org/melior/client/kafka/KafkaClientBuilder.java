/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.client.kafka;

/**
 * Convenience class for building a {@code KafkaClient}.  Provides
 * switches for asynchronous transport, Kerberos authentication and
 * secure connections.
 * @author Melior
 * @since 2.3
 */
public class KafkaClientBuilder {

    private boolean async = false;

    private boolean kerberos = false;

    private boolean ssl = false;

    /**
     * Constructor.
     */
    private KafkaClientBuilder() {

        super();
    }

    /**
     * Create Kafka client builder.
     * @return The Kafka client builder
     */
    public static KafkaClientBuilder create() {

        return new KafkaClientBuilder();
    }

    /**
     * Build Kafka client.
     * @return The Kafka client
     */
    public KafkaClient build() {

        return new KafkaClient(async, kerberos, ssl);
    }

    /**
     * Enable asynchronous transport.
     * @return The Kafka client builder
     */
    public KafkaClientBuilder async() {

        this.async = true;

        return this;
    }

    /**
     * Enable Kerberos.
     * @return The Kafka client builder
     */
    public KafkaClientBuilder kerberos() {

        this.kerberos = true;

        return this;
    }

    /**
     * Enable SSL.
     * @return The Kafka client builder
     */
    public KafkaClientBuilder ssl() {

        this.ssl = true;

        return this;
    }

}
