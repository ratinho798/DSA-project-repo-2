import ballerina/http;
import ballerina/io;

service /notify on new http:Listener(8085) {

    resource function post alert(http:Caller caller, http:Request req) returns error? {
        json msg = check req.getJsonPayload();
        // Send notification (simulate)
        io:println("Notification sent: ", msg.toJsonString());
        check caller->respond({"status": "sent"});
    }
}