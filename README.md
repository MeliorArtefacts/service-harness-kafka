# Melior Service Harness :: Kafka
<div style="display: inline-block;">
<img src="https://img.shields.io/badge/version-2.3-green?style=for-the-badge"/>
<img src="https://img.shields.io/badge/production-ready-green?style=for-the-badge"/>
<img src="https://img.shields.io/badge/compatibility-spring_boot_2.4.5-green?style=for-the-badge"/>
</div>

## Artefact
Get the artefact and the POM file in the *artefact* folder.
```
<dependency>
    <groupId>org.melior</groupId>
    <artifactId>melior-harness-kafka</artifactId>
    <version>2.3</version>
</dependency>
```

## Client
Create a bean to instantiate the Kafka client.
```
@Bean("myclient")
@ConfigurationProperties("myclient")
public KafkaClient client() {
    return KafkaClientBuilder.create().kerberos().ssl().build();
}
```

The Kafka client is auto-configured from the application properties.
```
myclient.url=tcp://some.service:9092
myclient.kerberos-config=file:my.krb5.conf
myclient.user-name=user
myclient.key-tab=file:my.keytab
myclient.topic=my.topic
myclient.request-timeout=30
myclient.inactivity-timeout=15

```

Wire in and use the Kafka client.
```
@Autowired
@Qualifier("myclient")
private KafkaClient client;

public void foo(Person person) throws RemotingException {
    client.send(person);
}
```

The Kafka client may be configured using these application properties.

|Name|Default|Description|
|:---|:---|:---|
|`url`||The URL of the Kafka server|
|`kerberos-config`||The path to the Kerberos configuration file.  Required when using Kerberos authentication|
|`user-name`||The user name required by the Kafka server.  Maps to the principal when using Kerberos authentication|
|`password`||The password required by the Kafka server.  Required when using plain text authentication|
|`key-tab`||The path to the Kerberos keytab file.  Required when using Kerberos authentication|
|`service-name`|kafka|The Kafka service name|
|`jaas`||Any supplimentary JAAS configuration that may be required|
|`key-store`||The path to the key store|
|`key-store-type`|jks|The type of the key store|
|`key-store-password`||The password which is required to access the key store|
|`key-password`||The password which is required to access the key pair in the key store.  A password should be set on the key pair and should at the very least be the same as the password to the key store|
|`trust-store`||The path to the trust store|
|`trust-store-type`|jks|The type of the trust store|
|`trust-store-password`||The password which is required to access the trust store|
|`topic`||The Kafka topic to produce messages to|
|`backoff-period`|1 s|The amount of time to back off when the circuit breaker trips|
|`backoff-limit`||The maximum amount of time to back off when the circuit breaker trips repeatedly|
|`request-timeout`|60 s|The amount of time to allow for a request to the Kafka server to complete|
|`inactivity-timeout`|300 s|The amount of time to allow before surplus connections to the Kafka server are pruned|

&nbsp;  
## References
Refer to the [**Melior Service Harness :: Core**](https://github.com/MeliorArtefacts/service-harness-core) module for detail on the Melior logging system and available utilities.
