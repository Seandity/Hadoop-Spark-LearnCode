###HBase
HBase是一个NoSQL存储系统，它是一个建立在Hadoop上面的数据库，它是使用Java来编写的
部署安装需要Hadoop和Zookeeper。支持数据的随机存储和读取。是一个面向列族的
数据库。可以用它来进行保存几百万列，数十亿行的数据，并且他可以部署在廉价的机器
上面，支持水平横向扩展。
HBase在存储数据的时候，首先会将数据写入到内存的缓冲区，等数据满了之后，
在往HDFS上面进行存储。写到HFile中，HFile对应列族，一个列族有可以对应多个
HFile.在集群的每个节点上，每个列族对应一个MemStore

Configuration config = HBaseConfiguration.create()
HTableInterface table = new HTable(config,"world")

HTablePool pool = new HTablePool()
HTableInterface table = pool.getTable("user")

Put put = new Put(Bytes.toBytes("ss"))
put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("Mark"))