package com.nttdata.poc;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.nttdata.poc.model.Activity;
import com.nttdata.poc.model.User;
import com.nttdata.poc.serializer.JsonSerdes;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

/**
 * Di base, questo mestiere dovrebbe essere fatto da un connettore, che mantiene allineato il contenuto di un topic.
 * Se non fosse possibile, si può adottare una soluzione del genere, che mantiene per un certo tempo il dato in cache
 * (state store), come una sorta di buffer e che pubblica il dato sul topic 'ufficiale' usato magari da KSQL
 * per materializzare il dato.
 */
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
public class OnDemandResourceRetriever {

    public static final String USER_BUFFER = "user-temp-store";

    String bootstrapServers;
    String sourceTopic;
    String destTopic;
    String resourceTopic;
    String stateDir;

    public static void main(String[] args) {
        String bootstrapServers = args[0];
        String sourceTopic = args[1];
        String destTopic = args[2];
        String resourceTopic = args[4];
        String stateDir = args[5];
        val resolver = new OnDemandResourceRetriever(bootstrapServers, sourceTopic, destTopic,
                resourceTopic, stateDir);
        resolver.start();
    }

    private void start() {
        val streams = new KafkaStreams(createTopology(), properties());
        streams.setUncaughtExceptionHandler(exception -> REPLACE_THREAD);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.REBALANCING) {
                // Do anything that's necessary to manage rebalance
                log.info("Rebalancing");
            }
        });
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private Topology createTopology() {

        // TODO DSL, anche se c'è da esplicitare il topic di changelog del buffer dell'utente

        Topology builder = new Topology();

        builder.addSource("Activity Without Recognized User",
                Serdes.String().deserializer(),
                JsonSerdes.activity().deserializer(),
                sourceTopic);

        // Buffer user data for 60 second
        Map<String, String> topicConfigs =
                Collections.singletonMap(RETENTION_MS_CONFIG, "60000");
        StoreBuilder<KeyValueStore<String, User>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(USER_BUFFER),
                        Serdes.String(),
                        JsonSerdes.user())
                        .withLoggingEnabled(topicConfigs);
        builder.addStateStore(storeBuilder, "User processor");

        builder.addProcessor("User retriever", UserProcessor::new, "Activity Without Recognized User");

        builder.addProcessor("Branch User", BranchProcessor::new, "User retriever");
        builder.addSink("Resource",
                resourceTopic,
                Serdes.String().serializer(), JsonSerdes.user().serializer(),
                "Branch User");

        builder.addProcessor("Forward Processor", ForwardProcessor::new, "User retriever");
        builder.addSink("Activity With Buffered User",
                destTopic,
                Serdes.String().serializer(), JsonSerdes.activity().serializer(),
                "Forward Processor");

        return builder;
    }

    @Value
    static class ActivityAndUser {
        Activity activity;
        User user;
    }

    public class BranchProcessor implements Processor<String, ActivityAndUser, String, User> {
        private ProcessorContext<String, User> context;

        @Override
        public void process(Record<String, ActivityAndUser> record) {
            val activityAndUser = record.value();
            log.info("BranchProcessor - Received {}", activityAndUser);
            if (activityAndUser.user != null) {
                context.forward(new Record<>(activityAndUser.activity.getUserId(), activityAndUser.user, record.timestamp()));
            }
        }

        @Override
        public void init(ProcessorContext<String, User> context) {
            this.context = context;
        }

        @Override
        public void close() {
        }
    }

    public class ForwardProcessor implements Processor<String, ActivityAndUser, String, Activity> {
        private ProcessorContext<String, Activity> context;

        @Override
        public void process(Record<String, ActivityAndUser> record) {
            val activityAndUser = record.value();
            log.info("ForwardProcessor - Received {}", activityAndUser);
            context.forward(new Record<>(activityAndUser.activity.getActivityId(),
                    activityAndUser.activity, record.timestamp()));
        }

        @Override
        public void init(ProcessorContext<String, Activity> context) {
            this.context = context;
        }

        @Override
        public void close() {
        }
    }

    public class UserProcessor implements Processor<String, Activity, String, ActivityAndUser> {

        private ProcessorContext<String, ActivityAndUser> context;
        private KeyValueStore<String, User> kvStore;

        @Override
        public void process(Record<String, Activity> record) {
            Optional.of(kvStore.get(record.value().getUserId()))
                    .ifPresentOrElse(user -> {
                                log.info("User {} in temporary buffer", record.value().getUserId());
                                // Se trovo l'utente nel buffer, arricchisco il dato e lo passo avanti
                                val activity = record.value();
                                fillActivityWithUser(activity, user);
                                context.forward(new Record<>(record.key(), new ActivityAndUser(record.value(), null),
                                        record.timestamp()));
                            },
                            () -> {
                                log.info("User {} NOT PRESENT in temporary buffer", record.value().getUserId());
                                retrieveResource(record.value().getUserId())
                                        .ifPresentOrElse(user -> {
                                                    val activity = record.value();
                                                    fillActivityWithUser(activity, user);
                                                    kvStore.put(activity.getUserId(), user);
                                                    context.forward(new Record<>(record.key(),
                                                            new ActivityAndUser(record.value(), user),
                                                            record.timestamp()));
                                                },
                                                () -> {
                                                    // TODO SEND TO DLQ, RISORSA NON INDIVIDUATA
                                                });
                            });

        }

        private Optional<User> retrieveResource(String userId) {
            // TODO, CHIAMATA A LDAP, POSSIAMO FARE MOCK PER TEST
            return Optional.of(User.builder().build());
        }

        private void fillActivityWithUser(Activity activity, User user) {
            activity.setCommonName(user.getCn());
            activity.setRoomNumber(user.getRoomNumber());
            activity.setEmail(user.getEmail());
        }

        @Override
        public void init(ProcessorContext<String, ActivityAndUser> context) {
            this.context = context;
            this.kvStore = context.getStateStore(USER_BUFFER);

            try {
                URL url = Resources.getResource("employeeNumbers.txt");
                String employeeNumbers = Resources.toString(url, Charsets.UTF_8);
            }
            catch (Exception e) {
            }

        }

        @Override
        public void close() {
            Processor.super.close();
        }
    }

    private Properties properties() {
        val properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "resource-retriever");
        return properties;
    }
}
