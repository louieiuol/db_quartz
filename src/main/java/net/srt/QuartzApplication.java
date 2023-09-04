package net.srt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * 定时任务模块
 *
 * @author 阿沐 babamu@126.com
 */
@EnableFeignClients
@EnableDiscoveryClient
@SpringBootApplication
public class QuartzApplication {

	public static void main(String[] args) {
		SpringApplication.run(QuartzApplication.class, args);
	}

}
