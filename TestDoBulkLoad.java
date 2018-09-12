import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huangteng on 2017/7/26.
 */
public class TestDoBulkLoad {
    private static Logger logger = LoggerFactory.getLogger(TestDoBulkLoad.class);

    public static void main(String[] args) throws Exception {
        try {
            Configuration conf = HBaseConfiguration.create();
//            Configuration conf = new Configuration();
//            conf.set("bdp.hbase.erp", "huangteng1");
//            conf.set("bdp.hbase.instance.name", "SL1000000002865");
//            conf.set("bdp.hbase.accesskey", "MZYH5UIKEY3BUD3ZU3XVFEZROA");
//            Connection connection = ConnectionFactory.createConnection(conf);
            Connection connection = HConnectionManager.createConnection(conf);

            logger.info("--------conf:{}", conf);

            String hFilePath = "/test/hfiles";
//            conf.set("hbase.zookeeper.quorum","BJYF-HBASE-KANGAROO-16025.hadoop.jd.local:2181,BJYF-HBASE-KANGAROO-14154.hadoop.jd.local:2181,BJYF-HBASE-KANGAROO-14163.hadoop.jd.local:2181");
//            conf.set("zookeeper.znode.parent","/hbase_kangaroo");
            LoadIncrementalHFiles loadFiles = new LoadIncrementalHFiles(conf);
            loadFiles.doBulkLoad(new Path(hFilePath), (HTable)connection.getTable(TableName.valueOf("test:testT")));
            //loadFiles.doBulkLoad(new Path(hFilePath),connection.getAdmin(),connection.getTable(TableName.valueOf("test:testT")),connection.getRegionLocator(TableName.valueOf("test:testT")));
           // loadFiles.doBulkLoad(new Path(hFilePath),new HTable(conf,"test:TestTable"));
            logger.info("---finish---");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
