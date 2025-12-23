package io.sustc;

import io.sustc.benchmark.BenchmarkService;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.shell.ShellApplicationRunner;

@SpringBootApplication
@Slf4j
public class Application {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(Application.class);
        // 强制以 Web (Servlet) 模式启动，避免进入 Spring Shell
        app.setWebApplicationType(WebApplicationType.SERVLET);
        app.run(args);
    }

    @EventListener
    public void onApplicationReady(ApplicationStartedEvent event) {
        val ctx = event.getApplicationContext();
        val runnerBeans = ctx.getBeansOfType(ShellApplicationRunner.class);
        val benchmarkServiceBeans = ctx.getBeansOfType(BenchmarkService.class);
        log.debug("{} {}", runnerBeans, benchmarkServiceBeans);
        if (runnerBeans.size() != 1 || benchmarkServiceBeans.size() != 1) {
            log.error("Failed to verify beans");
            SpringApplication.exit(ctx, () -> 1);
        }
    }
}