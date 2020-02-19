# auto-hdfs
Check folders and automatically synchronize to hdfs
This project can monitor a folder and synchronize with the hdfs automatically
#about apps
apps提供了这个项目的jar包以及依赖的所有jar包，以便直接打开使用。

how to use
1. 保证系统中已安装了jdk1.8及以上版本。
2. 新建一个文件夹用于存放同步hdfs的数据。注意这个文件夹不宜随意挪动。
3. 利用解压工具打开jar包，找到application.yml文件，修改参数并替换原有文件，然后启动jar包即可。
4. 参数说明：
    hdfs.address：连接hdfs的地址。
    hdfs.localPath：将hdfs数据同步到本地的文件夹绝对路径。
    hdfs.localCheckTime：本地文件夹监控间隔时间。
    hdgs.hdfsCheckTime：监控hdfs的间隔时间。