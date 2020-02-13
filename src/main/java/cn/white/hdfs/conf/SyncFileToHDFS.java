package cn.white.hdfs.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author GrainRain
 * @date 2020/02/13 11:18
 **/
@Component
public class SyncFileToHDFS {
    @Value("${hdfs.localPath}")
    private String localPath;
    @Autowired
    private FileSystem fileSystem;

    private List<String> allLocalFiles = new LinkedList<>();

    private Configuration configuration = new Configuration();

    //从hadoop获取文件
    public void getFromHDFS(String filePath) throws IOException {
        //文档输出地址
        String outPath = localPath + filePath;
        System.out.println("获取文件到本地，添加到本地路径:"+outPath);
        int last = outPath.lastIndexOf("/");
        String dir = outPath.substring(0,last);
        //如果不存在，创建文件夹
        File file = new File(dir);
        if(!file.exists())
            file.mkdirs();

        FSDataInputStream in = fileSystem.open(new Path(filePath));
        FileOutputStream out = new FileOutputStream(outPath);
        //从hdfs获取并写出文件
        IOUtils.copyBytes(in,out,configuration);
        in.close();
    }

    //在hdfs创建文件夹
    public void mkdirs(String hdfsDirPath) throws IOException {
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
        //获取在hdfs上的路径
        String hdfsFilePath = localFilePath.substring(localPath.length());
        System.out.println("上传本地文件到hdfs，本地路径为："+localFilePath+"hdfs路径为："+hdfsFilePath);
        if(hdfsFileExists(hdfsFilePath))
            throw new RuntimeException("路径为>>"+hdfsFilePath+"<<的hdfs文件已存在");
        String dirPath = hdfsFilePath.substring(0,hdfsFilePath.lastIndexOf("/"));
        if (dirPath.length()<1)
            dirPath = "/";
        //判断hdfs是否存在文件上级目录，不存在则创建后再上传文件
        if(!hdfsFileExists(dirPath))
            this.mkdirs(dirPath);
        FSDataOutputStream out = fileSystem.create(new Path(hdfsFilePath));
        FileInputStream in = new FileInputStream(localFilePath);
        IOUtils.copyBytes(in,out,configuration);
        System.out.println("上传成功，文件上传路径为>>"+hdfsFilePath);
        in.close();
    }

    //删除文件
    public void deleteFile(String localFilePath) throws IOException {
        System.out.println("开始执行删除文件操作：删除路径为>>"+localFilePath);
        String hdfsFilePath = "/" + localFilePath.substring(localPath.length()+1);
        if(!hdfsFileExists(hdfsFilePath)) {
            System.out.println("不存在的文件：路径为>>"+hdfsFilePath);
            return;
        }
        fileSystem.delete(new Path(hdfsFilePath),true);
        System.out.println("删除成功：路径为>>"+ hdfsFilePath);
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
    public RemoteIterator<LocatedFileStatus> getAllHDFSFile(String hdfsDirPath) throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path(hdfsDirPath), true);
        return locatedFileStatusRemoteIterator;
    }

    //获取本地目录的所有文件
    private List<String> getLocalFiles(String localDirPath){
        File f = new File(localDirPath);
        File[] files = f.listFiles();
        for(File file:files){
            if(!file.isDirectory())
                allLocalFiles.add(file.getPath());
            else
                getLocalFiles(file.getPath());
        }
        return allLocalFiles;
    }

    public void clearList(){
        allLocalFiles.clear();
    }

    public List<String> getAllLocalFile(String localDirPath){
        List<String> localFiles = new LinkedList<>(getLocalFiles(localDirPath));
        this.clearList();
        return localFiles;
    }
}
