/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.client.kafka;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Constructs a new Kafka {@code ProducerRecord} object from a text payload.
 * <p>
 * This implementation sets the correlation id in the message headers
 * to the transaction identifier from the transaction context, to allow
 * a transaction to be traced to the Kafka server.
 * @author Melior
 * @since 2.3
 */
public abstract class ProducerRecordFactory {

    /**
     * Create producer record.
     * @param topic The message topic
     * @param payload The message payload
     * @param correlationId The correlation identifier
     * @return The producer record
     */
    public static ProducerRecord<String, String> create(
        final String topic,
        final String payload,
        final String correlationId) {

        ProducerRecord<String, String> producerRecord;

        producerRecord = new ProducerRecord<String, String>(topic, payload);
        producerRecord.headers().add("requestID", correlationId.getBytes());

        return producerRecord;
    }

}
