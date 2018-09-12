

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huangteng on 2017/5/5.
 */
public class TestWrite {

    private static Logger logger = LoggerFactory.getLogger(TestWrite.class);

    public static void main(String[] args) {
        try {
            logger.info("---start!---");
            //String defaultzk = "172.16.160.25,172.19.141.63,172.19.141.54";
            int defaultrowkey = 100;
//            if(null!=args[0])
//                defaultzk = args[0];
            if(null!=args[0])
                defaultrowkey = Integer.parseInt(args[0]);
//            System.setProperty("hadoop.home.dir", "D:\\software\\hadoop-3.0.0-alpha2");
            Configuration configuration = new Configuration();
            configuration.set("bdp.hbase.erp", "huangteng1");//erp
            configuration.set("bdp.hbase.instance.name", "SL1000000002865");//实例
            configuration.set("bdp.hbase.accesskey", "MZYH5UIKEY3BUD3ZU3XVFEZROA");//AccessKey
           // configuration.set("conf.switch.zookeeper.quorum",defaultzk);
           // configuration.set("hbase.zookeeper.quorum", "192.168.178.95,192.168.178.96,192.168.178.97,192.168.178.98,");
            Connection connection = ConnectionFactory.createConnection(configuration);//保持单例
//            try {
//                AccessControlClient.grant(connection,"ns1","SL1000", Permission.Action.READ, Permission.Action.WRITE);
//            } catch (Throwable throwable) {
//                throwable.printStackTrace();
//            }
            Table table = connection.getTable(TableName.valueOf("test:testT"));

            Increment increment = new Increment(Bytes.toBytes("2008"));
            table.increment(increment);
            BufferedMutator bufferedMutator = connection.getBufferedMutator(TableName.valueOf("test:TestTable"));
          //  List<Put> putList = new LinkedList<Put>();
            for (int i = 0; i < 10; i++) {
                Put put = new Put(Bytes.toBytes(String.valueOf(defaultrowkey + i)));
                put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("age"), Bytes.toBytes("16"));
                table.put(put);
                logger.info("i={}",i+defaultrowkey);
                Thread.sleep(1000L);
               // putList.add(put);
            }
           // table.put(putList);
//            bufferedMutator.mutate(put);
            logger.info("---finish put data!---");
        } catch (Exception e) {
            logger.info("-----exception-----:{}", e);
        } finally {
            logger.info("---over---");
        }
    }
}
