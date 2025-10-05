import ballerina/http;
import ballerinax/mongodb;

mongodb:Client mongoClient = check new({
    uri: "mongodb://mongo:27017",
    database: "smart_ticketing_system"
});

service /transport on new http:Listener(8082) {

    resource function post createRoute(http:Caller caller, http:Request req) returns error? {
        json route = check req.getJsonPayload();
        check mongoClient->insert("routes", route);
        check caller->respond({"message": "Route created"});
    }

    resource function post updateSchedule(http:Caller caller, http:Request req) returns error? {
        json scheduleUpdate = check req.getJsonPayload();
        // Publish to Kafka topic schedule.updates (not implemented)
        check caller->respond({"message": "Schedule update published"});
    }
}