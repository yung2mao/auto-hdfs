package cn.white.hdfs.conf;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

/**
 * @author GrainRain
 * @date 2020/02/13 11:09
 **/
@Configuration
public class HDFSFactory {
    @Value("${hdfs.address}")
    private String address;

    @Bean
    public FileSystem createFileSystem(){
        try {
            return FileSystem.get(URI.create(address),new org.apache.hadoop.conf.Configuration(),"root");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    @Bean("fileList")
    public List<String> fileList(){
        return new LinkedList<String>();
    }
}
