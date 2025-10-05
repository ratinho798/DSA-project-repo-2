import ballerina/http;
import ballerinax/kafka;
import ballerinax/mongodb;

mongodb:Client mongoClient = new({
    uri: "mongodb://mongo:27017",
    database: "smart_ticketing_system"
});

kafka:Producer ticketProducer = check new({
    topic: "ticket.requests",
    bootstrapServers: "kafka:9092"
});

service /ticketing on new http:Listener(8083) {

    resource function post requestTicket(http:Caller caller, http:Request req) returns error? {
        json ticketRequest = check req.getJsonPayload();
        check mongoClient->insert("tickets", ticketRequest);
        check ticketProducer->send(ticketRequest.toJsonString());
        check caller->respond({"message": "Ticket request sent"});
    }
}