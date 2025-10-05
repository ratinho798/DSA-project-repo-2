import ballerina/http;
import ballerinax/mongodb;

mongodb:Client mongoClient = check new({
    uri: "mongodb://mongo:27017",
    database: "smart_ticketing_system"
});

service /admin on new http:Listener(8086) {

    resource function get ticketReports(http:Caller caller) returns error? {
        json tickets = check mongoClient->find("tickets", {});
        check caller->respond(tickets);
    }

    resource function post serviceUpdate(http:Caller caller, http:Request req) returns error? {
        json update = check req.getJsonPayload();
        check caller->respond({"message": "Service update published"});
    }
}