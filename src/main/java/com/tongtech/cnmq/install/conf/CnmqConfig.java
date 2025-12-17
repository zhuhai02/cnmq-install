package com.tongtech.cnmq.install.conf;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "cnmq")
public class CnmqConfig {

    private String deployType;

    private String installType;

    private String installPackagePath;

    private String shellPath;

    private String metaShellPath;

    private String clusterName;

    private String licenseIps;

    private String licensePublicKey;

    private ClusterOnSameNode  clusterOnSameNode;

    private ClusterOnDiffNode clusterOnDiffNode;

    @Data
    public static class ClusterOnSameNode {
        private String ip;
        private List<NodeConfig> nodeConfigs;
        private AppConfigMap configMap;
    }

    @Data
    public static class NodeConfig {
        private String installPath;
        private ZkInfo zkInfo;
        private BookieInfo bookieInfo;
        private BrokerInfo brokerInfo;
    }


    @Data
    public static class ClusterOnDiffNode {
        private String installPath;
        private int zkClientPort;
        private int zkSyncDataPort;
        private int zkElectionPort;
        private int zkAdminPort;
        private int zkMetricsPort;
        private List<String> zkIps;

        private String licenseIps;
        private String licensePublicKey;
        private int bookieMetricsPort;
        private int bookieServerPort;
        private List<String> bookieIps;

        private int brokerAdminPort;
        private int brokerServerPort;
        private List<String> brokerIps;
        private AppConfigMap configMap;
    }

    @Data
    public static class AppConfigMap {
        private Map<String, String> zk;
        private Map<String, String> bookie;
        private Map<String, String> broker;
    }

    @Data
    public static class ZkInfo {
        private String installPath;
        private String ip;
        private int clientPort;
        private int syncDataPort;
        private int electionPort;
        private int adminPort;
        private int metricsPort;
        private int myId;
    }

    @Data
    public static class BookieInfo {
        private String installPath;
        private String ip;
        private int metricsPort;
        private int serverPort;
    }

    @Data
    public static class BrokerInfo {
        private String installPath;
        private String ip;
        private int adminPort;
        private int serverPort;
    }
}
