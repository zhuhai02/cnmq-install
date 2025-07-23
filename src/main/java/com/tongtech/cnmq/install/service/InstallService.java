package com.tongtech.cnmq.install.service;

import com.tongtech.cnmq.install.conf.CnmqConfig;
import com.tongtech.cnmq.install.constant.BookieConfigKey;
import com.tongtech.cnmq.install.constant.BrokerConfigKey;
import com.tongtech.cnmq.install.constant.LicenseConfigKey;
import com.tongtech.cnmq.install.constant.ZkConfigKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

@Slf4j
@Component
public class InstallService {

    @Value("${server.port}")
    private Integer serverPort;

    private final CnmqConfig cnmqConfig;

    private final List<String> localIps;

    private List<CnmqConfig.ZkInfo> zkInfos;

    private List<CnmqConfig.BookieInfo>  bookieInfos;

    private List<CnmqConfig.BrokerInfo> brokerInfos;

    private Map<String, String> configServerMap = new HashMap<>();

    private String bookieZkConfig;

    private String brokerZkConfig;

    private String initializeMetadataShellPath;

    private String metadataStore;

    private String webServiceUrl;

    private String brokerServiceUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    private final AtomicInteger requestCount = new AtomicInteger(0);

    private ExecutorService executorService = Executors.newCachedThreadPool();

    private final ApplicationContext context;

    private final AtomicBoolean pass = new AtomicBoolean(true);
    public InstallService(CnmqConfig cnmqConfig, ApplicationContext context) {
        this.cnmqConfig = cnmqConfig;
        this.context = context;
        localIps = getLocalIp();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() throws IOException, InterruptedException {
        startInstall();
    }

    public void startInstall() throws IOException, InterruptedException {
        log.info("================开始部署=====================");
        if (CollectionUtils.isEmpty(localIps)) {
            log.warn("部署失败，未获取到本地IP地址");
            return;
        }
        log.info("================解析配置=====================");
        // 部署前准备，加载部署参数
        prepare();
        if (!pass.get()) {
            return;
        }
        log.info("================解压安装包=====================");
        // 准备安装路径
        prepareInstallPath();
        log.info("================更新配置文件=====================");
        // 更新配置文件
        updateAllConfigFile();
        log.info("30秒后开始启动zk");
        Thread.sleep(30000);
        // 启动zk
        startInstallZk();
        // 初始化元数据
        initializeClusterMetadata();
        if (!pass.get()) {
            return;
        }

        // 等待元素据初始化完成
        waitMetaInitialized();
        if (!pass.get()) {
            return;
        }
        // 启动bookie
        startInstallBookie();

        // 校验bookie是否启全部启动完成

        // 启动broker
        startInstallBroker();

        // 校验broker是否完全启动
        log.info("集群安装完毕，请检测是否正常");

        System.exit(SpringApplication.exit(context, () -> 0));

    }

    private void waitMetaInitialized() {
        String zkAddr;
        String deployType = cnmqConfig.getDeployType();
        if ("clusterOnDiffNode".equals(deployType)) {
            StringBuilder stringBuilder = new StringBuilder();
            for (String zkIp : cnmqConfig.getClusterOnDiffNode().getZkIps()) {
                stringBuilder.append(zkIp)
                        .append(":")
                        .append(cnmqConfig.getClusterOnDiffNode().getZkClientPort())
                        .append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            zkAddr = stringBuilder.toString();
        } else {
            StringBuilder stringBuilder = new StringBuilder();
            for (CnmqConfig.NodeConfig nodeConfig : cnmqConfig.getClusterOnSameNode().getNodeConfigs()) {
                stringBuilder.append(cnmqConfig.getClusterOnSameNode().getIp())
                        .append(":")
                        .append(nodeConfig.getZkInfo().getClientPort())
                        .append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            zkAddr = stringBuilder.toString();
        }
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        String nodePath = "/admin";

        try {
            ZooKeeper zk = new ZooKeeper(zkAddr, 3000, watchedEvent -> {
                if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            });

            connectedSignal.await();
            boolean initialized = false;
            for (int i = 0; i < 60; i++) {
                initialized = checkNodeExists(zk, nodePath);
                if (initialized) {
                    break;
                }
                Thread.sleep(10000);
            }
            if (!initialized) {
                pass.set(initialized);
            }
            zk.close();
        } catch (IOException | InterruptedException | KeeperException e) {
            log.error("检测元素据执行出错", e);
            pass.set(false);
        }
    }

    private boolean checkNodeExists(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);
        return stat != null;
    }

    /**
     * 等zk全部启动
     * @throws InterruptedException
     */
    private void waitZkStarted() throws InterruptedException {
        String deployType = cnmqConfig.getDeployType();
        if ("clusterOnDiffNode".equals(deployType)) {
            int zkClientPort = cnmqConfig.getClusterOnDiffNode().getZkClientPort();
            List<String> zkIps = cnmqConfig.getClusterOnDiffNode().getZkIps();
            CountDownLatch countDownLatch = new CountDownLatch(zkIps.size());
            for (String zkIp : zkIps) {
                executorService.execute(() -> {
                    boolean result = false;
                    for (int i = 0; i < 60; i++) {
                        result = checkZooKeeperStatus(zkIp, zkClientPort, 3000);
                        if (result) {
                            break;
                        }
                        try {
                            log.info("10秒后尝试重新检测zk");
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            log.error("异常", e);
                        }
                    }
                    if (!result) {
                        pass.set(false);
                    }
                    countDownLatch.countDown();
                });
            }
            countDownLatch.await();
        } else {
            CountDownLatch countDownLatch = new CountDownLatch(this.zkInfos.size());
            for (CnmqConfig.ZkInfo zkInfo : this.zkInfos) {
                executorService.execute(() -> {
                    boolean result = false;
                    for (int i = 0; i < 60; i++) {
                        result = checkZooKeeperStatus(zkInfo.getIp(), zkInfo.getClientPort(), 3000);
                        if (result) {
                            break;
                        }
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            log.error("异常", e);
                        }
                    }
                    if (!result) {
                        pass.set(false);
                    }
                    countDownLatch.countDown();
                });
            }
            countDownLatch.await();
        }
    }

    private void prepareInstallPath() throws IOException {
        String deployType = cnmqConfig.getDeployType();
        String installPackagePath = this.cnmqConfig.getInstallPackagePath();
        switch (deployType) {
            case "clusterOnSameNode":
                List<CnmqConfig.NodeConfig> nodeConfigs = this.cnmqConfig.getClusterOnSameNode().getNodeConfigs();
                for (CnmqConfig.NodeConfig nodeConfig : nodeConfigs) {
                    extractTarGz(installPackagePath, nodeConfig.getInstallPath());
                }
                break;
            case "clusterOnDiffNode":
                extractTarGz(installPackagePath, this.cnmqConfig.getClusterOnDiffNode().getInstallPath());
                break;
            default:
                break;
        }
    }


    private void startInstallZk() throws IOException {
        for (CnmqConfig.ZkInfo zkInfo : this.zkInfos) {
            log.info("================启动zk=====================");
            String myIdPath = zkInfo.getInstallPath() + "data/zookeeper/myid";
            createMyid(myIdPath, zkInfo.getMyId());
            startApp(zkInfo.getInstallPath(), "zookeeper");
        }
    }

    private void startInstallBookie() throws IOException {
        for (CnmqConfig.BookieInfo bookieInfo : this.bookieInfos) {
            log.info("================启动bookie=====================");
            startApp(bookieInfo.getInstallPath(), "bookie");
        }
    }

    private void startInstallBroker() throws IOException {
        for (CnmqConfig.BrokerInfo brokerInfo : this.brokerInfos) {
            log.info("================启动broker=====================");
            startApp(brokerInfo.getInstallPath(), "broker");
        }

    }

    private void prepare() {
        String deployType = cnmqConfig.getDeployType();
        switch (deployType) {
            case "clusterOnSameNode":
                prepareSameNodeConfig();
                break;
            case "clusterOnDiffNode":
                prepareDiffNodeConfig();
                break;
            default:
                log.warn("不支持的部署类型：{} 可支持类型为clusterOnSameNode（集群部署在相同的节点机器）" +
                        "和 clusterOnDiffNode（集群部署在不同的节点机器）", deployType);
        }
    }


    /**
     * config文件赋值
     * @param configFilePath 配置文件路径
     * @param modifications 配置文件修改项
     */
    private void updateConfigFile(String configFilePath, Map<String, String> modifications) {
        try {
            File file = new File(configFilePath);
            List<String> lines = new ArrayList<>();

            // 读取所有行
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
            }

            // 修改配置行
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                for (Map.Entry<String, String> entry : modifications.entrySet()) {
                    String key = entry.getKey();
                    if (line.startsWith(key + "=")) {
                        lines.set(i, key + "=" + entry.getValue());
                        modifications.remove(key); // 移除已修改的项
                        break;
                    }
                }
            }

            // 添加新增配置项
            for (Map.Entry<String, String> entry : modifications.entrySet()) {
                lines.add(entry.getKey() + "=" + entry.getValue());
            }

            // 写入修改后的内容
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                for (String line : lines) {
                    writer.write(line);
                    writer.newLine();
                }
            }
            log.info("配置文件<{}>已成功修改！", configFilePath);
        } catch (IOException e) {
            log.error("配置文件<{}>修改失败", configFilePath, e);
        }
    }

    /**
     * 解压安装包到指定路径
     * @param source
     * @param destination
     * @throws IOException
     */
    private void extractTarGz(String source, String destination) throws IOException {
        // 创建目标目录（如果不存在）
        Path destPath = Paths.get(destination);
        Files.createDirectories(destPath);

        // 解压 .gz 文件
        try (FileInputStream fis = new FileInputStream(source);
             GZIPInputStream gis = new GZIPInputStream(fis);
             TarArchiveInputStream tis = new TarArchiveInputStream(gis)) {

            TarArchiveEntry entry;
            while ((entry = tis.getNextTarEntry()) != null) {
                // 获取原始条目名称并处理路径分隔符
                String originalName = entry.getName().replace('\\', '/');

                // 提取文件名（去除最顶层目录）
                String fileName = removeTopLevelDirectory(originalName);

                // 如果处理后的文件名为空，跳过（避免创建根目录）
                if (fileName.isEmpty()) {
                    continue;
                }

                // 构建新的目标路径
                Path entryPath = destPath.resolve(fileName);

                if (entry.isDirectory()) {
                    // 创建目录（包括父目录）
                    Files.createDirectories(entryPath);
                } else {
                    // 确保父目录存在
                    Files.createDirectories(entryPath.getParent());

                    // 写入文件
                    try (OutputStream fos = Files.newOutputStream(entryPath)) {
                        IOUtils.copy(tis, fos);
                    }
                }
            }
        }
        setExecutableForFiles(destination + "/bin");
    }

    // 移除最顶层目录，直接将文件解压到目标目录
    private String removeTopLevelDirectory(String path) {
        // 处理以斜杠开头的情况
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        // 查找第一个斜杠位置
        int firstSlashIndex = path.indexOf('/');
        if (firstSlashIndex != -1) {
            // 返回斜杠后的部分（即移除顶层目录）
            return path.substring(firstSlashIndex + 1);
        } else {
            // 如果没有斜杠，说明是文件，直接返回
            return path;
        }
    }

    /**
     * 设置目录下所有文件（不含子目录）的可执行权限
     */
    public static void setExecutableForFiles(String directoryPath) throws IOException {
        Path dir = Paths.get(directoryPath);

        // 检查目录是否存在
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            throw new IOException("目录不存在: " + directoryPath);
        }

        // 遍历目录下的直接子项（不递归）
        try (Stream<Path> stream = Files.list(dir)) {
            stream.filter(Files::isRegularFile) // 仅处理文件，不包含目录
                    .forEach(path -> {
                        try {
                            // 设置文件可执行权限
                            Files.setPosixFilePermissions(path,
                                    java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x"));

                        } catch (IOException e) {
                            log.error("可执行文件授权失败: " + path, e);
                        }
                    });
        }
    }

    /**
     * 更新所有配置文件
     */
    private void updateAllConfigFile() {
        updateZkConfigFile();
        updateBookieConfigFile();
        updateBrokerConfigFile();
    }

    /**
     * zk配置文件赋值
     */
    private void updateZkConfigFile() {
        String deployType = cnmqConfig.getDeployType();
        for (CnmqConfig.ZkInfo zkInfo : this.zkInfos) {
            Map<String, String> modifications = new HashMap<>();
            switch (deployType) {
                case "clusterOnSameNode":
                    if (cnmqConfig.getClusterOnSameNode().getConfigMap() != null
                            && cnmqConfig.getClusterOnSameNode().getConfigMap().getZk() != null) {
                        modifications.putAll(cnmqConfig.getClusterOnSameNode().getConfigMap().getZk());
                    }
                    break;
                case "clusterOnDiffNode":
                    if (cnmqConfig.getClusterOnDiffNode().getConfigMap() != null
                            && cnmqConfig.getClusterOnDiffNode().getConfigMap().getZk() != null) {
                        modifications.putAll(cnmqConfig.getClusterOnDiffNode().getConfigMap().getZk());
                    }
                    break;
                default:
                    break;
            }
            modifications.put(ZkConfigKey.CLIENT_PORT, String.valueOf(zkInfo.getClientPort()));
            modifications.put(ZkConfigKey.ADMIN_SERVER_PORT, String.valueOf(zkInfo.getAdminPort()));
            modifications.put(ZkConfigKey.METRICS_PROVIDER_HTTP_PORT, String.valueOf(zkInfo.getMetricsPort()));
            modifications.putAll(configServerMap);
            String configPath = zkInfo.getInstallPath() + "conf/zookeeper.conf";
            updateConfigFile(configPath, modifications);
        }
    }

    /**
     * bookie配置文件赋值
     */
    private void updateBookieConfigFile() {
        String deployType = cnmqConfig.getDeployType();
        for (CnmqConfig.BookieInfo bookieInfo : this.bookieInfos) {
            Map<String, String> modifications = new HashMap<>();
            switch (deployType) {
                case "clusterOnSameNode":
                    if (cnmqConfig.getClusterOnSameNode().getConfigMap() != null
                            && cnmqConfig.getClusterOnSameNode().getConfigMap().getBookie() != null) {
                        modifications.putAll(cnmqConfig.getClusterOnSameNode().getConfigMap().getBookie());
                    }
                    break;
                case "clusterOnDiffNode":
                    if (cnmqConfig.getClusterOnDiffNode().getConfigMap() != null
                            && cnmqConfig.getClusterOnDiffNode().getConfigMap().getBookie() != null) {
                        modifications.putAll(cnmqConfig.getClusterOnDiffNode().getConfigMap().getBookie());
                    }
                    break;
                default:
                    break;
            }
            modifications.put(BookieConfigKey.METADATA_SERVICE_URI,
                    bookieZkConfig);
            modifications.put(BookieConfigKey.ADVERTISED_ADDRESS,
                    bookieInfo.getIp());
            modifications.put(BookieConfigKey.BOOKIE_PORT,
                    String.valueOf(bookieInfo.getServerPort()));
            modifications.put(BookieConfigKey.PROMETHEUS_STATS_HTTP_PORT,
                    String.valueOf(bookieInfo.getMetricsPort()));
            modifications.put(LicenseConfigKey.LICENSE_IPS,
                    String.valueOf(cnmqConfig.getLicenseIps()));
            modifications.put(LicenseConfigKey.LICENSE_PUBLIC_KEY,
                    String.valueOf(cnmqConfig.getLicensePublicKey()));

            String configPath = bookieInfo.getInstallPath() + "conf/bookkeeper.conf";
            updateConfigFile(configPath, modifications);
        }
    }

    /**
     * broker配置文件赋值
     */
    private void updateBrokerConfigFile() {
        String deployType = cnmqConfig.getDeployType();
        for (CnmqConfig.BrokerInfo brokerInfo : this.brokerInfos) {
            Map<String, String> modifications = new HashMap<>();
            switch (deployType) {
                case "clusterOnSameNode":
                    if (cnmqConfig.getClusterOnSameNode().getConfigMap() != null
                            && cnmqConfig.getClusterOnSameNode().getConfigMap().getBroker() != null) {
                        modifications.putAll(cnmqConfig.getClusterOnSameNode().getConfigMap().getBroker());
                    }
                    break;
                case "clusterOnDiffNode":
                    if (cnmqConfig.getClusterOnDiffNode().getConfigMap() != null
                            && cnmqConfig.getClusterOnDiffNode().getConfigMap().getBroker() != null) {
                        modifications.putAll(cnmqConfig.getClusterOnDiffNode().getConfigMap().getBroker());
                    }
                    break;
                default:
                    break;
            }
            modifications.put(BrokerConfigKey.METADATA_STORE_URL,
                    brokerZkConfig);
            modifications.put(BrokerConfigKey.CONFIGURATION_METADATA_STORE_URL,
                    brokerZkConfig);
            modifications.put(BrokerConfigKey.ADVERTISED_ADDRESS,
                    brokerInfo.getIp());
            modifications.put(BrokerConfigKey.BROKER_SERVICE_PORT,
                    String.valueOf(brokerInfo.getServerPort()));
            modifications.put(BrokerConfigKey.WEB_SERVICE_PORT,
                    String.valueOf(brokerInfo.getAdminPort()));
            modifications.put(BrokerConfigKey.CLUSTER_NAME,
                    String.valueOf(cnmqConfig.getClusterName()));
            modifications.put(LicenseConfigKey.LICENSE_IPS,
                    String.valueOf(cnmqConfig.getLicenseIps()));
            modifications.put(LicenseConfigKey.LICENSE_PUBLIC_KEY,
                    String.valueOf(cnmqConfig.getLicensePublicKey()));
            String configPath = brokerInfo.getInstallPath() + "conf/broker.conf";
            updateConfigFile(configPath, modifications);
        }
    }

    /**
     * 部署前准备，准备在同一个节点机器上部署集群的配置
     */
    private void prepareSameNodeConfig() {
        if (!checkSameNodeConfig()) {
            return;
        }
        this.zkInfos = new ArrayList<>();
        this.bookieInfos = new ArrayList<>();
        this.brokerInfos = new ArrayList<>();
        CnmqConfig.ClusterOnSameNode clusterOnSameNode = cnmqConfig.getClusterOnSameNode();
        AtomicInteger atomicInteger = new AtomicInteger(1);
        StringBuilder bookieZKConfig = new StringBuilder("zk//");
        StringBuilder brokerZKConfig = new StringBuilder("zk:");
        StringBuilder webServiceUrlConfig = new StringBuilder("http://");
        StringBuffer brokerServiceUrlConfig = new StringBuffer("pulsar://");
        for (CnmqConfig.NodeConfig nodeConfig : clusterOnSameNode.getNodeConfigs()) {

            int myId = atomicInteger.getAndIncrement();
            CnmqConfig.ZkInfo zkInfo = buildZkInfo(nodeConfig.getInstallPath(),
                    myId, clusterOnSameNode.getIp(), nodeConfig.getZkInfo().getClientPort(),
                    nodeConfig.getZkInfo().getSyncDataPort(), nodeConfig.getZkInfo().getElectionPort(),
                    nodeConfig.getZkInfo().getMetricsPort(), nodeConfig.getZkInfo().getAdminPort());

            CnmqConfig.BookieInfo bookieInfo = buildBookieInfo(nodeConfig.getInstallPath(),
                    clusterOnSameNode.getIp(),
                    nodeConfig.getBookieInfo().getMetricsPort(),
                    nodeConfig.getBookieInfo().getServerPort());

            CnmqConfig.BrokerInfo brokerInfo = buildBrokerInfo(nodeConfig.getInstallPath(),
                    clusterOnSameNode.getIp(),
                    nodeConfig.getBrokerInfo().getAdminPort(),
                    nodeConfig.getBrokerInfo().getServerPort());

            bookieZKConfig.append(clusterOnSameNode.getIp())
                    .append(":")
                    .append(zkInfo.getClientPort())
                    .append(";");

            brokerZKConfig.append(clusterOnSameNode.getIp())
                    .append(":")
                    .append(zkInfo.getClientPort())
                    .append(",");

            webServiceUrlConfig.append(clusterOnSameNode.getIp())
                    .append(":")
                    .append(brokerInfo.getAdminPort())
                    .append(",");

            brokerServiceUrlConfig.append(clusterOnSameNode.getIp())
                    .append(":")
                    .append(brokerInfo.getServerPort())
                    .append(",");

            configServerMap.put("server." + myId, String.format("%s:%d:%d",
                    clusterOnSameNode.getIp(),
                    zkInfo.getSyncDataPort(),
                    zkInfo.getElectionPort()
            ));

            this.zkInfos.add(zkInfo);
            this.bookieInfos.add(bookieInfo);
            this.brokerInfos.add(brokerInfo);

        }

        bookieZKConfig.deleteCharAt(bookieZKConfig.length() - 1);
        bookieZKConfig.append("/ledgers");
        this.bookieZkConfig = bookieZKConfig.toString();

        brokerZKConfig.deleteCharAt(brokerZKConfig.length() - 1);
        this.brokerZkConfig = brokerZKConfig.toString();

        webServiceUrlConfig.deleteCharAt(brokerZKConfig.length() - 1);
        this.webServiceUrl= webServiceUrlConfig.toString();

        brokerServiceUrlConfig.deleteCharAt(brokerZKConfig.length() - 1);
        this.brokerServiceUrl= brokerServiceUrlConfig.toString();

        this.initializeMetadataShellPath = zkInfos.get(0).getInstallPath() + cnmqConfig.getMetaShellPath();
        this.metadataStore = this.brokerZkConfig;

    }

    /**
     * 校验同节点配置
     * @return 校验是否通过
     */
    private boolean checkSameNodeConfig() {
        CnmqConfig.ClusterOnSameNode clusterOnSameNode = cnmqConfig.getClusterOnSameNode();
        if (!isLocalIp(clusterOnSameNode.getIp())) {
            log.warn("部署失败，配置的ip地址验证不通过，非本机ip");
            pass.set(false);
        }
        List<CnmqConfig.NodeConfig> nodeConfigs = clusterOnSameNode.getNodeConfigs();
        if (CollectionUtils.isEmpty(nodeConfigs)) {
            log.warn("部署失败，未配置nodeConfigs");
            pass.set(false);
        }


        Set<Integer> ports = new HashSet<>();
        for (CnmqConfig.NodeConfig nodeConfig : nodeConfigs) {
            ports.add(nodeConfig.getZkInfo().getClientPort());
            if (!ports.add(nodeConfig.getZkInfo().getSyncDataPort())) {
                log.warn("部署失败，zk端口号syncDataPort<{}>已经被使用", nodeConfig.getZkInfo().getSyncDataPort());
                pass.set(false);
            }
            if (!ports.add(nodeConfig.getZkInfo().getElectionPort())) {
                log.warn("部署失败，zk端口号electionPort<{}>已经被使用", nodeConfig.getZkInfo().getElectionPort());
                pass.set(false);
            }
            if (!ports.add(nodeConfig.getZkInfo().getMetricsPort())) {
                log.warn("部署失败，zk端口号metricsPort<{}>已经被使用", nodeConfig.getZkInfo().getMetricsPort());
                pass.set(false);
            }
            if (!ports.add(nodeConfig.getZkInfo().getAdminPort())) {
                log.warn("部署失败，zk端口号adminPort<{}>已经被使用", nodeConfig.getZkInfo().getAdminPort());
                pass.set(false);
            }

            if (!ports.add(nodeConfig.getBookieInfo().getServerPort())) {
                log.warn("部署失败，bookie端口号serverPort<{}>已经被使用", nodeConfig.getBookieInfo().getServerPort());
                pass.set(false);
            }
            if (!ports.add(nodeConfig.getBookieInfo().getMetricsPort())) {
                log.warn("部署失败，bookie端口号metricsPort<{}>已经被使用", nodeConfig.getBookieInfo().getMetricsPort());
                pass.set(false);
            }

            if (!ports.add(nodeConfig.getBrokerInfo().getServerPort())) {
                log.warn("部署失败，broker端口号serverPort<{}>已经被使用", nodeConfig.getBrokerInfo().getServerPort());
                pass.set(false);
            }
            if (!ports.add(nodeConfig.getBrokerInfo().getAdminPort())) {
                log.warn("部署失败，broker端口号adminPort<{}>已经被使用", nodeConfig.getBrokerInfo().getAdminPort());
                pass.set(false);
            }
        }

        int timeout = 3000;
        for (Integer port : ports) {
            if (isPortOpen(port, timeout)) {
                log.warn("部署失败，端口 <{}> 被占用", port);
                pass.set(false);
            }
        }

        return pass.get();
    }

    /**
     * 部署前准备，准备在不同节点机器上部署集群的配置
     */
    private void prepareDiffNodeConfig() {
        if (!checkDiffNodeConfig()) {
            return;
        }
        this.zkInfos = new ArrayList<>();
        this.bookieInfos = new ArrayList<>();
        this.brokerInfos = new ArrayList<>();
        CnmqConfig.ClusterOnDiffNode clusterOnDiffNode = cnmqConfig.getClusterOnDiffNode();
        List<String> zkIps = clusterOnDiffNode.getZkIps();
        List<String> bookieIps = clusterOnDiffNode.getBookieIps();
        List<String> brokerIps = clusterOnDiffNode.getBrokerIps();
        String localZkIp = selectLocalIp(zkIps);
        if (StringUtils.hasText(localZkIp)) {
            CnmqConfig.ZkInfo zkInfo = buildZkInfo(clusterOnDiffNode.getInstallPath(),
                    zkIps.indexOf(localZkIp) + 1, localZkIp, clusterOnDiffNode.getZkClientPort(),
                    clusterOnDiffNode.getZkSyncDataPort(), clusterOnDiffNode.getZkElectionPort(),
                    clusterOnDiffNode.getZkMetricsPort(), clusterOnDiffNode.getZkAdminPort()
                    );
            if (zkInfo.getMyId() == 1) {
                this.initializeMetadataShellPath = zkInfo.getInstallPath() + cnmqConfig.getMetaShellPath();
                StringBuilder stringBuilder = new StringBuilder("zk:");
                for (String zkIp : zkIps) {
                    stringBuilder.append(zkIp)
                            .append(":")
                            .append(clusterOnDiffNode.getZkClientPort())
                            .append(",");
                }
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                this.metadataStore = stringBuilder.toString();

                StringBuilder webServiceUrlConfig = new StringBuilder("http://");
                for (String brokerIp : brokerIps) {
                    webServiceUrlConfig.append(brokerIp)
                            .append(":")
                            .append(clusterOnDiffNode.getBrokerAdminPort())
                            .append(",");
                }
                webServiceUrlConfig.deleteCharAt(stringBuilder.length() - 1);
                this.webServiceUrl = webServiceUrlConfig.toString();

                StringBuilder brokerServiceUrlConfig = new StringBuilder("pulsar://");
                for (String brokerIp : brokerIps) {
                    brokerServiceUrlConfig.append(brokerIp)
                            .append(":")
                            .append(clusterOnDiffNode.getBrokerServerPort())
                            .append(",");
                }
                brokerServiceUrlConfig.deleteCharAt(stringBuilder.length() - 1);
                this.brokerServiceUrl = brokerServiceUrlConfig.toString();
            }
            zkInfos.add(zkInfo);

            AtomicInteger atomicInteger = new AtomicInteger(1);
            for (String zkIp : zkIps) {
                configServerMap.put("server." + atomicInteger.getAndIncrement(),
                        String.format("%s:%d:%d",
                                zkIp,
                                clusterOnDiffNode.getZkSyncDataPort(),
                                clusterOnDiffNode.getZkElectionPort()
                        ));
            }
        }
        String localBookieIp = selectLocalIp(bookieIps);
        if (StringUtils.hasText(localBookieIp)) {
            CnmqConfig.BookieInfo localBookieInfo = buildBookieInfo(clusterOnDiffNode.getInstallPath(),
                    localBookieIp, clusterOnDiffNode.getBookieMetricsPort(),
                    clusterOnDiffNode.getBookieServerPort());
            bookieInfos.add(localBookieInfo);
            StringBuilder stringBuilder = new StringBuilder("zk://");
            for (String zkIp : zkIps) {
                stringBuilder.append(zkIp)
                        .append(":")
                        .append(clusterOnDiffNode.getZkClientPort())
                        .append(";");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            stringBuilder.append("/ledgers");
            this.bookieZkConfig = stringBuilder.toString();
        }
        String localBrokerIp = selectLocalIp(brokerIps);
        if (StringUtils.hasText(localBrokerIp)) {
            CnmqConfig.BrokerInfo localBrokerInfo = buildBrokerInfo(clusterOnDiffNode.getInstallPath(),
                    localBrokerIp, clusterOnDiffNode.getBrokerAdminPort(),
                    clusterOnDiffNode.getBrokerServerPort());
                    new CnmqConfig.BrokerInfo();
            brokerInfos.add(localBrokerInfo);
            StringBuilder stringBuilder = new StringBuilder("zk:");
            for (String zkIp : zkIps) {
                stringBuilder.append(zkIp)
                        .append(":")
                        .append(clusterOnDiffNode.getZkClientPort())
                        .append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            this.brokerZkConfig = stringBuilder.toString();
        }
    }


    private CnmqConfig.ZkInfo buildZkInfo(String installPath, int myId, String localZkIp,
                                          int clientPort, int syncDataPort, int electionPort,
                                          int metricsPort, int adminPort) {
        CnmqConfig.ZkInfo zkInfo = new CnmqConfig.ZkInfo();
        if (!installPath.endsWith("/")) {
            installPath += "/";
        }
        zkInfo.setInstallPath(installPath);
        zkInfo.setMyId(myId);
        zkInfo.setIp(localZkIp);
        zkInfo.setClientPort(clientPort);
        zkInfo.setSyncDataPort(syncDataPort);
        zkInfo.setElectionPort(electionPort);
        zkInfo.setMetricsPort(metricsPort);
        zkInfo.setAdminPort(adminPort);
        return zkInfo;
    }

    private CnmqConfig.BookieInfo buildBookieInfo(String installPath, String ip,
                                                  int metricsPort, int serverPort) {
        CnmqConfig.BookieInfo bookieInfo = new CnmqConfig.BookieInfo();
        if (!installPath.endsWith("/")) {
            installPath += "/";
        }
        bookieInfo.setInstallPath(installPath);
        bookieInfo.setIp(ip);
        bookieInfo.setMetricsPort(metricsPort);
        bookieInfo.setServerPort(serverPort);
        return bookieInfo;
    }

    private CnmqConfig.BrokerInfo buildBrokerInfo(String installPath, String ip,
                                                  int adminPort, int serverPort) {
        CnmqConfig.BrokerInfo brokerInfo = new CnmqConfig.BrokerInfo();
        if (!installPath.endsWith("/")) {
            installPath += "/";
        }
        brokerInfo.setInstallPath(installPath);
        brokerInfo.setIp(ip);
        brokerInfo.setAdminPort(adminPort);
        brokerInfo.setServerPort(serverPort);
        return brokerInfo;
    }

    /**
     * 校验不同节点配置
     * @return 校验是否通过
     */
    private boolean checkDiffNodeConfig() {
        CnmqConfig.ClusterOnDiffNode clusterOnDiffNode = cnmqConfig.getClusterOnDiffNode();
        List<String> zkIps = clusterOnDiffNode.getZkIps();
        List<String> bookieIps = clusterOnDiffNode.getBookieIps();
        List<String> brokerIps = clusterOnDiffNode.getBrokerIps();
        if (CollectionUtils.isEmpty(zkIps)) {
            log.warn("部署失败，未配置zk的ip地址");
            pass.set(false);
        }
        if (CollectionUtils.isEmpty(bookieIps)) {
            log.warn("部署失败，未配置bookie的ip地址");
            pass.set(false);
        }
        if (CollectionUtils.isEmpty(brokerIps)) {
            log.warn("部署失败，未配置broker的ip地址");
            pass.set(false);
        }
        if (zkIps.size() % 2 == 0) {
            log.warn("部署失败，zk部署数量必须是奇数");
            pass.set(false);
        }
        Set<String> zkIpSet = new HashSet<>(zkIps);
        if (zkIpSet.size() != zkIps.size()) {
            log.warn("部署失败，zk配置的Ip存在重复");
            pass.set(false);
        }
        Set<String> bookieIpSet = new HashSet<>(bookieIps);
        if (bookieIpSet.size() != bookieIps.size()) {
            log.warn("部署失败，bookie配置的Ip存在重复");
            pass.set(false);
        }
        Set<String> brokerIpSet = new HashSet<>(brokerIps);
        if (brokerIpSet.size() != brokerIps.size()) {
            log.warn("部署失败，broker配置的Ip存在重复");
            pass.set(false);
        }

        Map<String, List<Integer>> ip2portMap = new HashMap<>();
        for (String zkIp : zkIpSet) {
            ip2portMap.putIfAbsent(zkIp, new ArrayList<>());
            ip2portMap.get(zkIp).add(clusterOnDiffNode.getZkClientPort());
            ip2portMap.get(zkIp).add(clusterOnDiffNode.getZkSyncDataPort());
            ip2portMap.get(zkIp).add(clusterOnDiffNode.getZkElectionPort());
            ip2portMap.get(zkIp).add(clusterOnDiffNode.getZkMetricsPort());
            ip2portMap.get(zkIp).add(clusterOnDiffNode.getZkAdminPort());
        }
        for (String bookieIp : bookieIpSet) {
            ip2portMap.putIfAbsent(bookieIp, new ArrayList<>());
            ip2portMap.get(bookieIp).add(clusterOnDiffNode.getBookieMetricsPort());
            ip2portMap.get(bookieIp).add(clusterOnDiffNode.getBookieServerPort());
        }
        for (String brokerIp : brokerIpSet) {
            ip2portMap.putIfAbsent(brokerIp, new ArrayList<>());
            ip2portMap.get(brokerIp).add(clusterOnDiffNode.getBrokerAdminPort());
            ip2portMap.get(brokerIp).add(clusterOnDiffNode.getBrokerServerPort());
        }

        ip2portMap.forEach((ip, ports) -> {
            Set<Integer> portSet = new HashSet<>(ports);
            if (ports.size() != portSet.size()) {
                log.warn("部署失败，ip<{}>使用了重复的端口", ip);
                pass.set(false);
            }
            checkMultiplePorts(ip, portSet.stream().toList(), pass);
        });
        return pass.get();
    }

    /**
     * 调用API检测多个端口
     */
    public void checkMultiplePorts(String host, List<Integer> ports, AtomicBoolean pass) {
        String url = "http://" + host + ":" + serverPort + "/api/port/batch-check";

        // 设置请求头
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // 创建请求实体
        HttpEntity<List<Integer>> requestEntity = new HttpEntity<>(ports, headers);

        try {
            // 发送POST请求
            ResponseEntity<Map> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    requestEntity,
                    Map.class
            );

            if (response.getStatusCode() == HttpStatus.OK) {
                Map<String, Object> result = response.getBody();
                assert result != null;
                List<Integer> openPortList = (List<Integer>) result.get("openPortList");

                openPortList.forEach(port -> {
                    log.warn("部署失败，ip<{}>的端口<{}>已经被占用", host, port);
                    pass.set(false);
                });
            } else {
                if (requestCount.getAndIncrement() < 60) {
                    log.info("端口检测请求调用失败，10秒后重新请求尝试检测端口是否被占用");
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        log.error("线程中断异常", e);
                    }
                    checkMultiplePorts(host, ports, pass);
                }
                pass.set(false);
                log.warn("部署失败，端口检测接口未被正常处理，无法检测端口是否被占用");
            }
        } catch (RestClientException e) {
            if (requestCount.getAndIncrement() < 60) {
                log.info("端口检测请求调用失败，10秒后重新请求尝试检测端口是否被占用");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException error) {
                    log.error("线程中断异常", error);
                }
                checkMultiplePorts(host, ports, pass);
            }
        }
    }




    private String selectLocalIp(List<String> ips) {
        Set<String> set = new HashSet<>(ips);
        if (set.size() != ips.size()) {
            return null;
        }
        List<String> localIps = new ArrayList<>();
        for (String ip : ips) {
            if (isLocalIp(ip)) {
                localIps.add(ip);
            }
        }
        if (localIps.isEmpty()) {
            return null;
        }
        if (localIps.size() > 1) {
            return null;
        }
        return localIps.get(0);
    }
    /**
     * 检测配置的ip是否是本机可用的ip
     * @return 是否是本机ip
     */
    private boolean isLocalIp(String useIp) {
        return localIps.contains(useIp);
    }

    /**
     * 获取ipv4对应的ip地址
     * @return 网卡ipv4地址
     */
    private List<String> getLocalIp() {
        List<String> ips = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            log.info("===== 检测到的网络接口及IP地址 =====");

            while (interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();

                // 过滤掉未启用或虚拟的接口
                if (!ni.isUp() || ni.isLoopback() || ni.isVirtual()) {
                    continue;
                }

                log.info("网卡: {} ({})", ni.getDisplayName(), ni.getName());
                Enumeration<InetAddress> addresses = ni.getInetAddresses();

                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();

                    // 过滤回环地址，保留IPv4
                    if (!addr.isLoopbackAddress()) {
                        String ip = addr.getHostAddress();
                        if (addr instanceof Inet4Address) {
                            ips.add(ip);
                        }
                    }
                }
            }
        } catch (SocketException e) {
            log.error("获取网络接口信息时出错: ", e);
        }
        return ips;
    }

    private boolean isPortOpen(int port, int timeout) {
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress("localhost", port), timeout);
            return true; // 连接成功，端口开放
        } catch (IOException e) {
            return false; // 连接失败，端口未开放或被防火墙阻止
        }
    }


    private void startApp(String installPath, String appName) throws IOException {
        // 要执行的脚本路径
        String executablePath = installPath + cnmqConfig.getShellPath();

        // 构建命令及其参数
        List<String> command = new ArrayList<>();
        command.add(executablePath);
        command.add("start");         // 位置参数1
        command.add(appName);         // 位置参数2

        // 创建进程构建器
        ProcessBuilder processBuilder = new ProcessBuilder(command);

        // 可选：设置工作目录
        // processBuilder.directory(new File("/path/to/working/directory"));

        // 启动进程
        processBuilder.start();
    }

    private void initializeClusterMetadata() throws IOException, InterruptedException {
        // 等待zk是否完全启动
        waitZkStarted();
        if (!pass.get()) {
            return;
        }

        if ("clusterOnDiffNode".equals(cnmqConfig.getDeployType())
                && !StringUtils.hasText(this.metadataStore)) {
            return;
        }
        log.info("================初始化元数据=====================");
        // 构建命令及其参数
        List<String> command = new ArrayList<>();
        command.add(initializeMetadataShellPath);
        command.add("initialize-cluster-metadata");
        command.add("--cluster");
        command.add(cnmqConfig.getClusterName());
        command.add("--metadata-store");
        command.add(metadataStore);
        command.add("--configuration-metadata-store");
        command.add(metadataStore);
        command.add("--web-service-url");
        command.add(webServiceUrl);
        command.add("--broker-service-url");
        command.add(brokerServiceUrl);

        // 创建进程构建器
        ProcessBuilder processBuilder = new ProcessBuilder(command);

        // 可选：设置工作目录
        // processBuilder.directory(new File("/path/to/working/directory"));

        // 启动进程
        processBuilder.start();
    }

    private void createMyid(String filePath, int myid) {

        try {
            // 创建父目录（如果不存在）
            Path path = Paths.get(filePath);
            Files.createDirectories(path.getParent());

            // 写入内容到文件
            Files.write(path, String.valueOf(myid).getBytes(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
        }
    }

    /**
     * 检查 ZooKeeper 服务状态
     * @param host ZooKeeper 服务器主机名或 IP
     * @param port ZooKeeper 服务器端口
     * @param timeout 连接超时时间（毫秒）
     * @return 如果服务正常返回 true，否则返回 false
     */
    private boolean checkZooKeeperStatus(String host, int port, int timeout) {
        try (Socket socket = new Socket()) {
            // 建立连接
            socket.connect(new java.net.InetSocketAddress(host, port), timeout);

            // 发送四字命令 "ruok"（Are you OK?）
            OutputStream out = socket.getOutputStream();
            out.write("ruok".getBytes(StandardCharsets.UTF_8));
            out.flush();
            // 读取响应
            InputStream in = socket.getInputStream();
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            // 处理响应
            if (bytesRead > 0) {
                String response = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8).trim();
                return "imok".equalsIgnoreCase(response);
            }
            return false;
        } catch (IOException e) {
            log.error("连接 ZooKeeper<{}:{}> 失败: ", host, port, e);
            return false;
        }
    }
}
