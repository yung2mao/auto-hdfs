package cn.white.hdfs.service;

import cn.white.hdfs.conf.DirPath;
import cn.white.hdfs.conf.SyncFileToHDFS;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * @author GrainRain
 * @date 2020/02/13 18:34
 **/
@Service
public class CheckHDFSFile extends Thread{
    @Autowired
    private DirPath dirPath;

    @Autowired
    private SyncFileToHDFS syncFileToHDFS;

    @Autowired
    private Semaphore semaphore;

    @Override
    public void run(){
        try {
            semaphore.acquire();
            CheckHDFSFileWithLocal();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            semaphore.release();
        }
    }

    //同步HDFS数据到本地
    public void CheckHDFSFileWithLocal() throws IOException {
        RemoteIterator<LocatedFileStatus> allFile = syncFileToHDFS.getAllHDFSFile("/");
        int count = 0;
        while (allFile.hasNext()){
            count ++;
            String hdfsFilePath = allFile.next().getPath().toString();
            String filePath = "/"+hdfsFilePath.substring(dirPath.getHdfsAddress().length()+1);
            String localFilePath = dirPath.getLocalPath() + filePath;
            boolean localFileExists = syncFileToHDFS.localFileExists(localFilePath);
            if(!localFileExists)
                syncFileToHDFS.getFromHDFS(filePath);
        }
        System.out.println("HDFS数据成功同步到本地，当前文档数量为>> "+count);
    }
}
