
import ballerina/http;
import ballerinax/kafka as kafka;
import ballerinax/mongodb as mongo;
import ballerina/log;
import ballerina/random;
import ballerina/time;

public type TicketRequest record {
    string ticketId;
    string passengerId;
    decimal amount;
    string currency?;
    string ticketType?;
    string createdAt?;
};

public type PaymentRecord record {
    string _id?;
    string ticketId;
    string passengerId;
    decimal amount;
    string currency?;
    string status; 
    string paymentGatewayTxId?;
    string processedAt;
    string reason?;
};

string kafkaBootstrap = checkpanic env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
string topicTicketRequests = checkpanic env.get("KAFKA_TOPIC_TICKET_REQUESTS", "ticket.requests");
string topicPaymentsProcessed = checkpanic env.get("KAFKA_TOPIC_PAYMENTS_PROCESSED", "payments.processed");
string kafkaConsumerGroup = checkpanic env.get("KAFKA_CONSUMER_GROUP", "payment-service-group");
string mongoUrl = checkpanic env.get("MONGO_URL", "mongodb://localhost:27017");
string mongoDbName = checkpanic env.get("MONGO_DB", "transport_ticketing");
string mongoCollection = checkpanic env.get("MONGO_COLLECTION", "payments");
int httpPort = checkpanic int:parse(env.get("HTTP_PORT", "8082"));

/*
  ============ MongoDB client ============
  Using ballerinax/mongodb
*/
mongo:Client mongodbClient = new (mongoUrl);

function getCollection() returns mongo:Collection|error {
    return mongodbClient->getCollection(mongoDbName, mongoCollection);
}

/*
  ============ Kafka producer ============
*/
kafka:ProducerConfiguration producerConfig = {
    bootstrapServers: kafkaBootstrap
};

kafka:Producer kafkaProducer = checkpanic new (producerConfig);

/*
  ============ Kafka consumer (listener) ============
  We'll create a separate worker that subscribes to ticket.requests and processes payments.
*/
kafka:ConsumerConfiguration consumerConfig = {
    bootstrapServers: kafkaBootstrap,
    groupId: kafkaConsumerGroup
};

kafka:Consumer kafkaConsumer = checkpanic new (consumerConfig);

/*
  Simple utility: current ISO timestamp
*/
function nowIso() returns string {
    time:Civil current = time:currentTime();
    return time:format(current, "yyyy-MM-dd'T'HH:mm:ssXXX");
}

/*
  Simulate payment gateway:
  Randomly succeed or fail. Returns (status, txId?, reason?)
*/
function simulatePaymentGateway(TicketRequest ticket) returns (string, string?, string?) {
    
    int seed = time:currentTime().microsecond;
    random:Random rng = new (seed);
    int v = rng.nextInt(1, 100);
    if (v <= 80) {
        string txId = "gw_" + v.toString() + "_" + time:currentTime().second.toString();
        return ("SUCCESS", txId, ());
    } else {
        string reason = "INSUFFICIENT_FUNDS";
        return ("FAILED", (), reason);
    }
}

/*
  Publish payments.processed event to Kafka
  payload is JSON string of PaymentRecord
*/
function publishPaymentProcessed(PaymentRecord payment) returns error? {
    json payload = {
        ticketId: payment.ticketId,
        passengerId: payment.passengerId,
        amount: payment.amount,
        currency: payment.currency ?: "NAD",
        status: payment.status,
        paymentGatewayTxId: payment.paymentGatewayTxId ?: (),
        processedAt: payment.processedAt,
        reason: payment.reason ?: ()
    };

    kafka:ProducerRecord record = {
        topic: topicPaymentsProcessed,
        value: payload.toString()
    };

    var sendRes = kafkaProducer->send(record);
    if (sendRes is error) {
        log:printError("Failed to publish payment processed event", 'error = sendRes);
        return sendRes;
    }
    log:printInfo("Published payments.processed for ticket " + payment.ticketId);
    return;
}

/*
  Persist payment into MongoDB
*/
function persistPayment(PaymentRecord payment) returns error? {
    var collRes = getCollection();
    if (collRes is error) {
        log:printError("Failed to get MongoDB collection", 'error = collRes);
        return collRes;
    }
    mongo:Collection coll = collRes;

    json doc = {
        ticketId: payment.ticketId,
        passengerId: payment.passengerId,
        amount: payment.amount,
        currency: payment.currency ?: "NAD",
        status: payment.status,
        paymentGatewayTxId: payment.paymentGatewayTxId ?: (),
        processedAt: payment.processedAt,
        reason: payment.reason ?: ()
    };

    var insertRes = coll->insertOne(doc);
    if (insertRes is error) {
        log:printError("Failed to persist payment", 'error = insertRes);
        return insertRes;
    }
    log:printInfo("Persisted payment record for ticket " + payment.ticketId);
    return;
}

/*
  Process a TicketRequest: simulate payment, persist, publish
*/
function processTicketPayment(TicketRequest ticket) returns error? {
    log:printInfo("Processing payment for ticket: " + ticket.ticketId);


    _ = time:sleep(200);

    (string status, string? txId, string? reason) = simulatePaymentGateway(ticket);

    PaymentRecord payment = {
        ticketId: ticket.ticketId,
        passengerId: ticket.passengerId,
        amount: ticket.amount,
        currency: ticket.currency,
        status: status,
        paymentGatewayTxId: txId,
        processedAt: nowIso(),
        reason: reason
    };

    var persistErr = persistPayment(payment);
    if (persistErr is error) {
        log:printError("Error persisting payment: " + persistErr.message());
    }

    var pubErr = publishPaymentProcessed(payment);
    if (pubErr is error) {
        log:printError("Error publishing payments.processed: " + pubErr.message());
        return pubErr;
    }

    return;
}

/*
  Kafka consumer worker
  Subscribe to ticket.requests
*/
service on new http:Listener(0) { } 
worker function kafkaConsumerWorker() {

    var subscribeRes = kafkaConsumer->subscribe([topicTicketRequests]);
    if (subscribeRes is error) {
        log:printError("Failed to subscribe to ticket.requests", 'error = subscribeRes);
        return;
    }
    log:printInfo("Subscribed to " + topicTicketRequests);

    while true {
        var pollRes = kafkaConsumer->poll(1000);
        if (pollRes is kafka:ConsumerRecord[]) {
            kafka:ConsumerRecord[] records = pollRes;
            foreach var rec in records {
                
                string valStr = "";
                if (rec.value is string) {
                    valStr = rec.value;
                } else if (rec.value is byte[]) {
                    valStr = string:fromBytes(rec.value);
                } else {
                    log:printError("Unknown kafka record value type");
                    continue;
                }

            
                var jsonParsed = checkpanic json:fromString(valStr);
                TicketRequest ticket = {
                    ticketId: (jsonParsed.ticketId).toString(),
                    passengerId: (jsonParsed.passengerId).toString(),
                    amount: decimal:fromString((jsonParsed.amount).toString()),
                    currency: jsonParsed.currency is () ? () : jsonParsed.currency.toString(),
                    ticketType: jsonParsed.ticketType is () ? () : jsonParsed.ticketType.toString(),
                    createdAt: jsonParsed.createdAt is () ? () : jsonParsed.createdAt.toString()
                };

    
                _ = processTicketPayment(ticket);
            }
        } else if (pollRes is error) {
            log:printError("Error polling Kafka", 'error = pollRes);

            _ = time:sleep(500);
        } else {
        }
    }
}

/*
  HTTP REST service for manual payment processing:
  POST /payment/process
  Body JSON: { "ticketId": "...", "passengerId":"...", "amount": 15.00, ... }
*/
service /payment on new http:Listener(httpPort) {

    resource function post process(http:Request req) returns http:Response|error {
        var jsonBody = req.getJsonPayload();
        if (jsonBody is error) {
            return http:Response(400, "Invalid or missing JSON body");
        }

        TicketRequest ticket = {
            ticketId: jsonBody.ticketId.toString(),
            passengerId: jsonBody.passengerId.toString(),
            amount: decimal:fromString(jsonBody.amount.toString()),
            currency: jsonBody.currency is () ? () : jsonBody.currency.toString(),
            ticketType: jsonBody.ticketType is () ? () : jsonBody.ticketType.toString(),
            createdAt: nowIso()
        };

        var err = processTicketPayment(ticket);
        if (err is error) {
            log:printError("Payment processing failed: " + err.message());
            json resp = { status: "ERROR", message: err.message() };
            return http:Response(500, resp.toString());
        }

        json resp = {
            status: "ACCEPTED",
            ticketId: ticket.ticketId,
            processedAt: nowIso()
        };
        return http:Response(202, resp.toString());
    }

    resource function get health() returns json {
        return { status: "ok", service: "payment-service", now: nowIso() };
    }
}
