package cn.white.hdfs.service;

import cn.white.hdfs.conf.DirPath;
import cn.white.hdfs.conf.SyncFileToHDFS;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author GrainRain
 * @date 2020/02/12 20:22
 **/
@Component
public class CheckFile implements ApplicationRunner {
    @Autowired
    private DirPath dirPath;
    @Autowired
    private SyncFileToHDFS syncFileToHDFS;

    @Autowired
    @Qualifier("fileList")
    private List<String> fileList;

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        this.CheckHDFSFileWithLocal();
        System.out.println("同步hdfs数据到本地成功");
        fileList = syncFileToHDFS.getAllLocalFile(dirPath.getLocalPath());
        System.out.println("同步本地数据到hdfs成功");
    }

    public void CheckHDFSFileWithLocal() throws IOException {
        RemoteIterator<LocatedFileStatus> allFile = syncFileToHDFS.getAllHDFSFile("/");
        while (allFile.hasNext()){
            String hdfsFilePath = allFile.next().getPath().toString();
            String filePath = "/"+hdfsFilePath.substring(dirPath.getHdfsAddress().length()+1);
            String localFilePath = dirPath.getLocalPath() + filePath;
            boolean localFileExists = syncFileToHDFS.localFileExists(localFilePath);
            if(!localFileExists)
                syncFileToHDFS.getFromHDFS(filePath);
        }
    }

    public void checkLocalFileChange() throws IOException {
        List<String> allLocalFile = syncFileToHDFS.getAllLocalFile(dirPath.getLocalPath());
        for(String localFile : allLocalFile){
            if(!fileList.contains(localFile)){
                syncFileToHDFS.putFile(localFile);
                fileList.add(localFile);
            }
        }
        if(allLocalFile.size() != fileList.size()){
            for(String fileFromList:fileList){
                if(!allLocalFile.contains(fileFromList)){
                    syncFileToHDFS.deleteFile(fileFromList);
                    fileList.remove(fileFromList);
                }
            }
        }
    }
}
