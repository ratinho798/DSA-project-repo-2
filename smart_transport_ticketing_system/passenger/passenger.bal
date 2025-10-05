import ballerina/http;
import ballerinax/kafka;
import ballerinax/mongodb;

mongodb:Client mongoClient = new({
    uri: "mongodb://mongo:27017",
    database: "smart_ticketing_system"
});

listener http:Listener passengerListener = new(8080);

// Kafka producer for ticket requests
kafka:Producer ticketProducer = new({
    bootstrapServers: "kafka:9092"
});

@http:ServiceConfig {
    basePath: "/passenger"
}
service passengerService on passengerListener {

    @http:ResourceConfig {
        path: "/register",
        methods: ["POST"]
    }
    resource function register(http:Caller caller, http:Request req) returns error? {
        var payload = check req.getJsonPayload();
        if payload is json {
            var result = mongoClient->insert("users", payload);
            if result is mongodb:InsertResult {
                check caller->respond({"message": "User registered successfully"});
            } else {
                check caller->respond({"error": result.message()});
            }
        }
    }

    @http:ResourceConfig {
        path: "/tickets",
        methods: ["GET"]
    }
    resource function getTickets(http:Caller caller, http:Request req) returns error? {
        var tickets = mongoClient->find("tickets", {});
        if tickets is json[] {
            check caller->respond(tickets);
        } else {
            check caller->respond({"error": "Could not fetch tickets"});
        }
    }
}