package com.nttdata.poc.serializer;


import com.nttdata.poc.model.Activity;
import com.nttdata.poc.model.Domain;
import com.nttdata.poc.model.User;
import lombok.val;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerdes {

    public static Serde<Activity> activity() {
        val serializer = new JsonSerializer<Activity>();
        val deserializer = new JsonDeserializer<>(Activity.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<User> user() {
        val serializer = new JsonSerializer<User>();
        val deserializer = new JsonDeserializer<>(User.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<Domain> domain() {
        val serializer = new JsonSerializer<Domain>();
        val deserializer = new JsonDeserializer<>(Domain.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
