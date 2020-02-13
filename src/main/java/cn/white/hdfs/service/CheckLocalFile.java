package cn.white.hdfs.service;

import cn.white.hdfs.conf.DirPath;
import cn.white.hdfs.conf.SyncFileToHDFS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * @author GrainRain
 * @date 2020/02/12 20:22
 **/
@Component
public class CheckLocalFile extends Thread{
    @Autowired
    private DirPath dirPath;

    @Autowired
    private SyncFileToHDFS syncFileToHDFS;

    @Autowired
    @Qualifier("fileList")
    private List<String> fileList;

    @Autowired
    private Semaphore semaphore;

    public void initLocalToHDFS() throws IOException {
        fileList = syncFileToHDFS.getAllLocalFile(dirPath.getLocalPath());
        System.out.println("init：初始化fileList成功，当前文件列表："+fileList);
        for(String filePath:fileList){
            String hdfsFilePath = filePath.substring(dirPath.getLocalPath().length());
            if(!syncFileToHDFS.hdfsFileExists(hdfsFilePath)){
                syncFileToHDFS.putFile(filePath);
            }
        }
        System.out.println("init: 本地数据成功同步到HDFS");
    }

    @Override
    public void run(){
        try {
            semaphore.acquire();
            checkLocalFileChange();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            semaphore.release();
        }

    }

/*    //同步HDFS数据到本地
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
    }*/
    //同步本地数据到HDFS
    public void checkLocalFileChange() throws IOException {
        List<String> allLocalFile = syncFileToHDFS.getAllLocalFile(dirPath.getLocalPath());
        for(String localFile : allLocalFile){
            if(!fileList.contains(localFile)){
                syncFileToHDFS.putFile(localFile);
                fileList.add(localFile);
            }
        }
        List<String> removeList = new LinkedList<>();
        if(allLocalFile.size() != fileList.size()){
            for(String fileFromList:fileList){
                if(!allLocalFile.contains(fileFromList)){
                    syncFileToHDFS.deleteFile(fileFromList);
                    removeList.add(fileFromList);
                }
            }
        }
        if(removeList.size() != 0){
            for(String removePath:removeList){
                fileList.remove(removePath);
            }
            removeList = null;
        }
        System.out.println("本地数据同步到HDFS完成，当前文档数量为>> "+allLocalFile.size());

    }
}
