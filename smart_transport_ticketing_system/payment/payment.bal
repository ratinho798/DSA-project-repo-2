import ballerina/http;
import ballerinax/kafka;
import ballerinax/mongodb;

mongodb:Client mongoClient = check new({
    uri: "mongodb://mongo:27017",
    database: "smart_ticketing_system"
});

kafka:Consumer paymentConsumer = check new({
    groupId: "payment-group",
    topics: ["ticket.requests"],
    bootstrapServers: "kafka:9092"
});

kafka:Producer paymentProducer = check new({
    topic: "payments.processed",
    bootstrapServers: "kafka:9092"
});

service /payment on new http:Listener(8084) {
    resource function post process(http:Caller caller, http:Request req) returns error? {
        json payment = check req.getJsonPayload();
        // Simulate payment
        check mongoClient->insert("payments", payment);
        check paymentProducer->send(payment.toJsonString());
        check caller->respond({"message": "Payment processed"});
    }
}