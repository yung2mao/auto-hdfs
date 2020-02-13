package cn.white.hdfs.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author GrainRain
 * @date 2020/02/13 16:55
 **/
@Service
public class SyncHDFSService implements ApplicationRunner {
    @Value("${hdfs.localCheckTime}")
    private int localCheckTime;

    @Value("${hdfs.hdfsCheckTime}")
    private int hdfsCheckTime;

    @Autowired
    private ScheduledExecutorService scheduledExecutorService;

    @Autowired
    private CheckLocalFile checkLocalFile;

    @Autowired
    private CheckHDFSFile checkHDFSFile;
    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        System.out.println("本地监控时间间隔>>"+localCheckTime+"s,hdfs监控时间间隔>>"+hdfsCheckTime+"s");
        init();
        checkLocalFile.initLocalToHDFS();
        scheduledExecutorService.scheduleWithFixedDelay(checkLocalFile,0,localCheckTime, TimeUnit.SECONDS);
        scheduledExecutorService.scheduleWithFixedDelay(checkHDFSFile,50,hdfsCheckTime,TimeUnit.SECONDS);
    }

    public void init() throws IOException {
        checkHDFSFile.CheckHDFSFileWithLocal();
        System.out.println("init: HDFS数据成功同步到本地");
    }
}
