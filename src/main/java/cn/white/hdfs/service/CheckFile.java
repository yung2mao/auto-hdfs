package cn.white.hdfs.service;

import cn.white.hdfs.conf.SyncFileToHDFS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * @author GrainRain
 * @date 2020/02/12 20:22
 **/
@Component
public class CheckFile implements ApplicationRunner {
    @Autowired
    private SyncFileToHDFS syncFileToHDFS;

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        boolean b = syncFileToHDFS.hdfsFileExists("/tx");
        System.out.println(b);
    }

    public void getAllHDFSFileAndCheck(){

    }
}
