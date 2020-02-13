package cn.white.hdfs.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author GrainRain
 * @date 2020/02/13 16:55
 **/
@Service
public class SyncHDFSService implements ApplicationRunner {
    @Autowired
    private ScheduledExecutorService scheduledExecutorService;
    @Autowired
    private CheckFile checkFile;
    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        checkFile.init();
        scheduledExecutorService.scheduleWithFixedDelay(checkFile,0,5, TimeUnit.SECONDS);
    }
}
