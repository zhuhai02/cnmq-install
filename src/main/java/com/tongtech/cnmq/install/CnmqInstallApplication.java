package com.tongtech.cnmq.install;

import com.tongtech.cnmq.install.conf.CnmqConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties(CnmqConfig.class)
@SpringBootApplication
public class CnmqInstallApplication {

    public static void main(String[] args) {
        SpringApplication.run(CnmqInstallApplication.class, args);
    }

}
