package cn.white.hdfs.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author GrainRain
 * @date 2020/02/13 11:18
 **/
@Component
public class SyncFileToHDFS {
    @Value("hdfs.localPath")
    private String localPath;
    @Autowired
    private FileSystem fileSystem;

    private Configuration configuration = new Configuration();

    //从hadoop获取文件
    public void getFromHDFS(String hdfsFilePath) throws IOException {
        //文档输出地址
        String outPath = localPath + hdfsFilePath;
        System.out.println("output dir:"+outPath);
        int last = outPath.lastIndexOf("/");
        String dir = outPath.substring(0,last);
        //如果不存在，创建文件夹
        File file = new File(dir);
        if(!file.exists())
            file.mkdirs();

        FSDataInputStream in = fileSystem.open(new Path(hdfsFilePath));
        FileOutputStream out = new FileOutputStream(outPath);
        //从hdfs获取并写出文件
        IOUtils.copyBytes(in,out,configuration);
        in.close();
    }

    //在hdfs创建文件夹
    public void mkdir(String hdfsDirPath) throws IOException {
        Path p = new Path(hdfsDirPath);
        boolean exists = hdfsFileExists(hdfsDirPath);

        if (!exists)
            fileSystem.mkdirs(p);
        System.out.println("create directory："+hdfsDirPath);
    }

    //上传文件
    public void putFile(String localFilePath) throws IOException {
        if(!localFileExists(localFilePath))
            throw new RuntimeException("路径为>>"+localFilePath+"<<的本地文件不存在");
        String hdfsFilePath = "/"+localFilePath.substring(localPath.length()+1);
        System.out.println("上传本地文件到hdfs，本地路径为："+localFilePath+"hdfs路径为："+hdfsFilePath);
        if(hdfsFileExists(hdfsFilePath))
            throw new RuntimeException("路径为>>"+hdfsFilePath+"<<的hdfs文件已存在");
        FSDataOutputStream out = fileSystem.create(new Path(hdfsFilePath));
        FileInputStream in = new FileInputStream(localFilePath);
        IOUtils.copyBytes(in,out,configuration);
        in.close();
    }

    //检查本地文件是否存在
    public boolean localFileExists(String localFilePath){
        File file = new File(localFilePath);
        return file.exists();
    }

    //判断hdfs指定全路径名文件是否存在
    public boolean hdfsFileExists(String hdfsFilePath) throws IOException {
        return fileSystem.exists(new Path(hdfsFilePath));
    }

    //递归获取hdfs所有文件信息

}
