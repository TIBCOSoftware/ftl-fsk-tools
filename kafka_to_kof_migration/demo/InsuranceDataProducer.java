import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Populates 10 insurance-domain Kafka topics with realistic JSON messages.
 *
 * Compile and run via demo/populate-kafka.sh, or manually:
 *   javac -cp $KAFKA_CLASSPATH InsuranceDataProducer.java
 *   java  -cp .:$KAFKA_CLASSPATH InsuranceDataProducer [--bootstrap-server host:port] [--messages N]
 */
public class InsuranceDataProducer {

    static final String[] TOPICS = {
        "insurance.auto.claims",
        "insurance.home.claims",
        "insurance.life.events",
        "insurance.health.claims",
        "insurance.commercial.claims",
        "insurance.policy.updates",
        "insurance.customer.profiles",
        "insurance.premium.payments",
        "insurance.fraud.alerts",
        "insurance.audit.log"
    };

    static final String[] CLAIM_STATUSES    = {"OPEN", "UNDER_REVIEW", "APPROVED", "DENIED", "PAID"};
    static final String[] INCIDENT_TYPES    = {"FIRE", "FLOOD", "THEFT", "VANDALISM", "WIND_DAMAGE"};
    static final String[] LIFE_EVENT_TYPES  = {"ENROLLMENT", "BENEFICIARY_CHANGE", "DEATH_CLAIM", "POLICY_LAPSE", "REINSTATEMENT"};
    static final String[] PROCEDURE_CODES   = {"99213", "99214", "99232", "36415", "93000", "70553", "27447", "43239"};
    static final String[] PAYMENT_METHODS   = {"ACH", "CHECK", "CREDIT_CARD", "WIRE_TRANSFER"};
    static final String[] DETECTION_METHODS = {"ML_MODEL", "RULES_ENGINE", "MANUAL_REVIEW", "THIRD_PARTY_FLAG"};
    static final String[] AUDIT_ACTIONS     = {"CREATE", "UPDATE", "DELETE", "APPROVE", "DENY", "LOGIN", "LOGOUT"};
    static final String[] AUDIT_RESOURCES   = {"claim", "policy", "customer", "payment", "document"};
    static final String[] UPDATE_TYPES      = {"RENEWAL", "AMENDMENT", "CANCELLATION", "REINSTATEMENT", "ENDORSEMENT"};
    static final String[] SEGMENTS          = {"STANDARD", "PREFERRED", "HIGH_VALUE", "NEW_TO_COMPANY"};

    static final String[] FIRST_NAMES = {
        "Alice","Bob","Carol","David","Eve","Frank","Grace","Henry","Iris","Jack",
        "Karen","Leo","Maria","Nathan","Olivia","Paul","Quinn","Rachel","Sam","Tara"
    };
    static final String[] LAST_NAMES = {
        "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Wilson","Taylor"
    };
    static final String[] CITIES = {
        "Chicago","Dallas","Phoenix","Philadelphia","San Antonio","San Diego","San Jose","Austin","Jacksonville","Columbus"
    };
    static final String[] PROVIDERS = {
        "General Hospital","City Medical Center","Riverside Clinic","St. Luke's","Mercy Health"
    };

    private final Random rng;
    private final SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat tsFmt   = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    InsuranceDataProducer(long seed) {
        this.rng = new Random(seed);
    }

    String pick(String[] arr) {
        return arr[rng.nextInt(arr.length)];
    }

    String name() {
        return pick(FIRST_NAMES) + " " + pick(LAST_NAMES);
    }

    String policyNumber() {
        return "POL-" + (100000 + rng.nextInt(900000));
    }

    String date(int maxDaysAgo) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_YEAR, -rng.nextInt(maxDaysAgo));
        return dateFmt.format(c.getTime());
    }

    String timestamp(int maxMinutesAgo) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MINUTE, -rng.nextInt(maxMinutesAgo));
        return tsFmt.format(c.getTime());
    }

    double amount(double min, double max) {
        return Math.round((min + (max - min) * rng.nextDouble()) * 100.0) / 100.0;
    }

    String ipAddress() {
        return "10." + rng.nextInt(256) + "." + rng.nextInt(256) + "." + rng.nextInt(256);
    }

    // ── Per-topic message generators ──────────────────────────────────────────

    String autoClaim(int i) {
        return String.format(
            "{\"claimId\":\"AC-%07d\",\"policyNumber\":\"%s\",\"driverName\":\"%s\"," +
            "\"accidentDate\":\"%s\",\"damageAmount\":%.2f,\"status\":\"%s\"," +
            "\"vehicleVin\":\"1HGBH41JXMN%06d\",\"atFault\":%s}",
            i, policyNumber(), name(), date(365), amount(500, 75000),
            pick(CLAIM_STATUSES), rng.nextInt(1000000), rng.nextBoolean());
    }

    String homeClaim(int i) {
        return String.format(
            "{\"claimId\":\"HC-%07d\",\"policyNumber\":\"%s\",\"ownerName\":\"%s\"," +
            "\"propertyAddress\":\"%d Main St, %s\",\"incidentType\":\"%s\"," +
            "\"estimatedLoss\":%.2f,\"reportedDate\":\"%s\",\"status\":\"%s\"}",
            i, policyNumber(), name(), 100 + rng.nextInt(9900), pick(CITIES),
            pick(INCIDENT_TYPES), amount(1000, 500000), date(180), pick(CLAIM_STATUSES));
    }

    String lifeEvent(int i) {
        return String.format(
            "{\"eventId\":\"LE-%07d\",\"policyNumber\":\"%s\",\"eventType\":\"%s\"," +
            "\"insuredName\":\"%s\",\"beneficiary\":\"%s\",\"coverageAmount\":%.2f," +
            "\"eventDate\":\"%s\"}",
            i, policyNumber(), pick(LIFE_EVENT_TYPES), name(), name(),
            amount(50000, 2000000), date(730));
    }

    String healthClaim(int i) {
        return String.format(
            "{\"claimId\":\"HH-%07d\",\"memberId\":\"MBR-%06d\",\"procedureCode\":\"%s\"," +
            "\"provider\":\"%s\",\"claimAmount\":%.2f,\"allowedAmount\":%.2f," +
            "\"claimDate\":\"%s\",\"status\":\"%s\"}",
            i, rng.nextInt(1000000), pick(PROCEDURE_CODES), pick(PROVIDERS),
            amount(50, 50000), amount(50, 30000), date(365), pick(CLAIM_STATUSES));
    }

    String commercialClaim(int i) {
        return String.format(
            "{\"claimId\":\"CC-%07d\",\"businessName\":\"%s Industries\",\"policyType\":\"BOP\"," +
            "\"lossDescription\":\"Business interruption due to %s\",\"claimValue\":%.2f," +
            "\"reportedDate\":\"%s\",\"status\":\"%s\"}",
            i, pick(LAST_NAMES), pick(INCIDENT_TYPES).toLowerCase().replace('_', ' '),
            amount(5000, 5000000), date(365), pick(CLAIM_STATUSES));
    }

    String policyUpdate(int i) {
        return String.format(
            "{\"updateId\":\"PU-%07d\",\"policyNumber\":\"%s\",\"updateType\":\"%s\"," +
            "\"effectiveDate\":\"%s\",\"expirationDate\":\"%s\",\"changedBy\":\"AGENT-%04d\"," +
            "\"premium\":%.2f}",
            i, policyNumber(), pick(UPDATE_TYPES), date(30), date(-365),
            rng.nextInt(10000), amount(500, 10000));
    }

    String customerProfile(int i) {
        String firstName = pick(FIRST_NAMES);
        String lastName  = pick(LAST_NAMES);
        return String.format(
            "{\"customerId\":\"CUST-%07d\",\"name\":\"%s %s\",\"email\":\"%s.%s@example.com\"," +
            "\"address\":\"%d Oak Ave, %s\",\"policyCount\":%d,\"segment\":\"%s\"," +
            "\"memberSince\":\"%s\"}",
            i, firstName, lastName, firstName.toLowerCase(), lastName.toLowerCase(),
            100 + rng.nextInt(9900), pick(CITIES),
            1 + rng.nextInt(5), pick(SEGMENTS), date(3650));
    }

    String premiumPayment(int i) {
        return String.format(
            "{\"paymentId\":\"PAY-%07d\",\"policyNumber\":\"%s\",\"amount\":%.2f," +
            "\"paymentDate\":\"%s\",\"paymentMethod\":\"%s\",\"confirmationCode\":\"CONF-%09d\"," +
            "\"status\":\"POSTED\"}",
            i, policyNumber(), amount(100, 5000), date(90),
            pick(PAYMENT_METHODS), rng.nextInt(1000000000));
    }

    String fraudAlert(int i) {
        return String.format(
            "{\"alertId\":\"FA-%07d\",\"claimId\":\"%s-%07d\",\"riskScore\":%.3f," +
            "\"detectionMethod\":\"%s\",\"flaggedAt\":\"%s\",\"reviewStatus\":\"PENDING\"," +
            "\"triggeredRules\":[\"RULE-%03d\",\"RULE-%03d\"]}",
            i, pick(new String[]{"AC","HC","HH","CC"}), rng.nextInt(1000000),
            rng.nextDouble(), pick(DETECTION_METHODS), timestamp(10080),
            rng.nextInt(500), rng.nextInt(500));
    }

    String auditLog(int i) {
        return String.format(
            "{\"auditId\":\"AUD-%07d\",\"userId\":\"USER-%05d\",\"action\":\"%s\"," +
            "\"resource\":\"%s\",\"resourceId\":\"%s-%06d\",\"timestamp\":\"%s\"," +
            "\"ipAddress\":\"%s\",\"success\":%s}",
            i, rng.nextInt(100000), pick(AUDIT_ACTIONS), pick(AUDIT_RESOURCES),
            pick(AUDIT_RESOURCES).toUpperCase(), rng.nextInt(1000000),
            timestamp(43200), ipAddress(), rng.nextBoolean());
    }

    String generate(String topic, int i) {
        switch (topic) {
            case "insurance.auto.claims":        return autoClaim(i);
            case "insurance.home.claims":        return homeClaim(i);
            case "insurance.life.events":        return lifeEvent(i);
            case "insurance.health.claims":      return healthClaim(i);
            case "insurance.commercial.claims":  return commercialClaim(i);
            case "insurance.policy.updates":     return policyUpdate(i);
            case "insurance.customer.profiles":  return customerProfile(i);
            case "insurance.premium.payments":   return premiumPayment(i);
            case "insurance.fraud.alerts":       return fraudAlert(i);
            case "insurance.audit.log":          return auditLog(i);
            default: return "{\"error\":\"unknown topic\"}";
        }
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = "localhost:9092";
        int messagesPerTopic = 1000;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bootstrap-server":   bootstrap = args[++i]; break;
                case "--messages-per-topic": messagesPerTopic = Integer.parseInt(args[++i]); break;
                default: System.err.println("Unknown argument: " + args[i]); System.exit(1);
            }
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");

        InsuranceDataProducer gen = new InsuranceDataProducer(42L);

        System.out.printf("Sending %d messages to each of %d topics on %s%n%n",
            messagesPerTopic, TOPICS.length, bootstrap);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String topic : TOPICS) {
                for (int i = 1; i <= messagesPerTopic; i++) {
                    String key   = String.format("%s-%07d", topic.substring(topic.lastIndexOf('.') + 1).toUpperCase(), i);
                    String value = gen.generate(topic, i);
                    producer.send(new ProducerRecord<>(topic, key, value));
                    if (i % 100 == 0 || i == messagesPerTopic) {
                        System.out.printf("  %-40s  %d/%d%n", topic, i, messagesPerTopic);
                    }
                }
                producer.flush();
            }
        }

        int total = TOPICS.length * messagesPerTopic;
        System.out.printf("%nDone. %d messages sent across %d topics.%n", total, TOPICS.length);
    }
}
