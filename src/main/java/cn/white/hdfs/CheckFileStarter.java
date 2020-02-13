package cn.white.hdfs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.CountDownLatch;

/**
 * @author GrainRain
 * @date 2020/02/13 11:06
 **/
@SpringBootApplication
public class CheckFileStarter {
    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        SpringApplication.run(CheckFileStarter.class);
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
