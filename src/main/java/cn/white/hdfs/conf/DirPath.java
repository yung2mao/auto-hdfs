package cn.white.hdfs.conf;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author GrainRain
 * @date 2020/02/13 14:44
 **/
@Component
public class DirPath {
    @Value("${hdfs.localPath}")
    private String localPath;
    @Value("${hdfs.address}")
    private String hdfsAddress;

    public String getLocalPath() {
        return localPath;
    }

    public void setLocalPath(String localPath) {
        this.localPath = localPath;
    }

    public String getHdfsAddress() {
        return hdfsAddress;
    }

    public void setHdfsAddress(String hdfsAddress) {
        this.hdfsAddress = hdfsAddress;
    }

    @Override
    public String toString() {
        return "DirPath{" +
                "localPath='" + localPath + '\'' +
                ", hdfsAddress='" + hdfsAddress + '\'' +
                '}';
    }
}
