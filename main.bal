import ballerina/http;
import ballerinax/kafka;

final string TICKET_STATUS_TOPIC = "ticket.status";
final string SCHEDULE_UPDATES_TOPIC = "schedule.updates";

service / on new http:Listener(8085) {

    // Manual notification trigger (via REST)
    resource function get notify/[string userId]() returns json {
        string message = "Manual notification sent to user: " + userId;
        io:println(message);
        return { status: "success", message };
    }
}

// Kafka consumer configuration
kafka:ConsumerConfiguration consumerConfig = {
    bootstrapServers: "kafka:9092",
    groupId: "notification-service-group",
    topics: [TICKET_STATUS_TOPIC, SCHEDULE_UPDATES_TOPIC],
    autoCommit: true
};

// Kafka service listens for events
service on new kafka:Listener(consumerConfig) {

    remote function onMessage(kafka:ConsumerRecord[] records) returns error? {
        foreach var record in records {
            string topic = record.topic;
            string msg = record.value.toString();
            io:println("📢 Notification received from topic [" + topic + "]: " + msg);

            // Simulated notification (could be email/SMS)
            if topic == TICKET_STATUS_TOPIC {
                io:println("✅ Ticket update sent to passenger: " + msg);
            } else if topic == SCHEDULE_UPDATES_TOPIC {
                io:println("⚠️ Schedule update sent to passenger: " + msg);
            }
        }
    }
}
