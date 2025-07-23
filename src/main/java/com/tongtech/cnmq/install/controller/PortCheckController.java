package com.tongtech.cnmq.install.controller;

import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/port")
public class PortCheckController {

    /**
     * 检测单个端口是否开放
     */
    @GetMapping("/check")
    public Map<String, Object> checkSinglePort(
            @RequestParam int port
    ) {
        Map<String, Object> result = new HashMap<>();
        boolean isOpen = isPortOpen(port);

        result.put("isOpen", isOpen);
        return result;
    }

    /**
     * 检测多个端口是否开放
     */
    @PostMapping("/batch-check")
    public Map<String, Object> checkMultiplePorts(
            @RequestBody List<Integer> ports
    ) {
        Map<String, Object> result = new HashMap<>();
        List<Integer> openPortList = new ArrayList<>();
        for (int port : ports) {
            if (isPortOpen(port)) {
                openPortList.add(port);
            }
        }

        result.put("openPortList", openPortList);

        return result;
    }

    private boolean isPortOpen(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress("localhost", port), 3000);
            return true; // 连接成功，端口开放
        } catch (IOException e) {
            return false; // 连接失败，端口未开放或被防火墙阻止
        }
    }
}