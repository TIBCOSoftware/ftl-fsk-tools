import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.clients.admin.Config;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaDescribeAllBrokersExample {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:5663");

        try (AdminClient adminClient = AdminClient.create(properties)) {
            // 1. Fetch all active nodes (brokers) in the cluster
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            Collection<Node> nodes = clusterResult.nodes().get();

            System.out.println("=== Active Cluster Nodes ===");
            for (Node node : nodes) {
                // 1. Get unique broker identifier
                int id = node.id();
                String idString = node.idString(); // Returns ID as a String
    
                // 2. Get network details
                String host = node.host();
                int port = node.port();
    
                // 3. Check for availability zone / rack setup (returns null if not configured)
                String rack = node.rack(); 
                String rackOutput = (rack != null) ? rack : "None configured";

                // Print out the details
                System.out.printf("Broker ID: %-5d | Host: %-20s | Port: %-6d | Rack: %s%n", 
                        id, host, port, rackOutput);
            }

            // 2. Map broker IDs to ConfigResource objects of type BROKER
            List<ConfigResource> brokerResources = nodes.stream()
                .map(node -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node.id())))
                .collect(Collectors.toList());

            // 3. Request configurations for all discovered brokers at once
            DescribeConfigsResult configsResult = adminClient.describeConfigs(brokerResources);

            // 4. Iterate through the results per broker
            for (ConfigResource resource : brokerResources) {
                System.out.println("\n--- Configurations for Broker ID: " + resource.name() + " ---");
                Config config = configsResult.values().get(resource).get();
                
                config.entries().forEach(entry -> {
                    // Filtering for non-default or sensitive configs can save console space
                    if (entry.value() != null) {
                        System.out.printf("%s = %s%n", entry.name(), entry.value());
                    }
                });
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

