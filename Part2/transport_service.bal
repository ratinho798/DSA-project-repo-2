import ballerina/http;
import ballerina/kafka;
import ballerina/log;
import ballerina/io;

type Route record {|
int id;
string name;
string description?;
|};

type Trip record {|
int id;
int routeId;
string departureTime;
string status;
|};

type Schedule record {|
int id;
int tripId;
string startTime;
string endTime?;
int capacity; 
|};

// in-memory stores

map<Route> routes = {};
map<Trip> trips = {};
map<Schedule> schedules = {};

int routeCounter = 1;
int tripCounter = 1;
int scheduleCounter = 1;
// producer
kafka:Producer kafkaProducer = check new (Kafka:DEFAULT_URL, {
    clientId: "transport-service"
});

service/transport on new http:Listener(8081) {
    resource function post routes(Route route) returns Route {
        route.id = routeCounter++;
        routes[route.id.toString()] = route;
        return route;
    }
    resource function get routes()  returns Route[] {
        return routes.values();
    }
    resource function post trips (Trip trip) returns Trip|error {
        if routes[trip.routeId.toString()] is () {
            return error("invalid routeId");
        }
        trip.id = tripCounter++;
        trip.status = "scheduled";
        trips[trip.id.toString()] = trip;

        json  event = {
            action: "created",
            tripId: trip.id,
            routeId: trip.routeId,
            departureTime: trip.departureTime
        };

        check kafkaProducer->send({topic: "schedule.updates", value: event.toJsonString()});
        return trip;
    }

    resource function post schedules(Schedule schedule) returns Schedule|error {
        if trips[schedule.tripId.toString()] is () {
            return error ("Invalid tripId");      
        }
        schedule.id = scheduleCounter++;
        schedules[schedule.id.toString()] = schedule;

        json event = {
            action: "created",
            scheduleId: schedule.id,
            tripId: schedule.tripId,
            capacity: schedule.capacity
        };
        check kafkaProducer->send({topic: "schedule.updates", value: event.toJsonString()});
        return schedule;

    }
}