import ballerina/http;
import ballerina/kafka;
import ballerina/log;
import ballerina/io;

type ScheduleEvent report {|
string action;
json payload;
string timestamp;
|};

type Alert report {|
int id;
string type;
string message;
string timestamp;
|};

map<ScheduleEvent>  consumedEvents = {};
map<Alert> alerts = {};
int eventCounter = 1;
int alertCounter = 1;

kafka:Consumer kafkaConsumer check new (kafka:DEFAULT_URL, {
    clientId: "admin-service"
});

service/admin on new https:Listener(8082) {
    resource function get reports/schedules() returns json {
        return { totalEvents: consumedEvents.lenght(), events: consumedEvents.values() };
    }
    //POST
    resource function post disruptions(Alert alert) returns Alert|error{
       alert.id = alertCounter++;
       alert.timestamp = time:toString(time:currentTime());
       alerts[alert.id.toString()] = alert;

       check kafkaProducer->send({ topic:"disruprion.alerts",
       value: alert.toJsonString()
       });
       return alert;
        resource function get alerts() returns Alert[] {
        return alerts.values();
    }
}

// Task in the background: consume Kafka messages
isolated function consume_Schedules() {
    while true {
        var result = kafkaConsumer->poll(1);
        if result is kafka:ConsumerRecord[] {
            foreach var record in result {
                var jsonMessage = checkpanic record.value.toJson();
                consumedEvents[eventCounter.toString()] = {
                    action: jsonMsg["action"].toString(),
                    payload: jsonMessage["payload"],
                    timestamp: jsonMessage["timestamp"].toString()
                };
                eventCounter += 1;
                log:printInfo("Consumed schedule update: " + record.value.toString());
            }
        }
    }
}

public function main() {
    // Run Kafka consumer loop here
    consume_Schedules();
}
    }
