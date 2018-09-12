import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * Created by huangteng on 2017/7/26.
 */
public class TestBulkLoad {
    private static Logger logger = LoggerFactory.getLogger(TestBulkLoad.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        try {
           // Configuration conf = HBaseConfiguration.create();
            Configuration conf = new Configuration();
//            conf.set("bdp.hbase.erp", "huangteng1");
//            conf.set("bdp.hbase.instance.name", "SL1000000003029");
//            conf.set("bdp.hbase.accesskey", "MZYH5UIKEY3BUB2QI3KHPIBGTM");
//            Connection connection = ConnectionFactory.createConnection(conf);
            conf.set("hbase.zookeeper.quorum","KC-HBASE-HERMES-10-196-83-73.hadoop.jd.local:2181,KC-HBASE-HERMES-10-196-83-74.hadoop.jd.local:2181,KC-HBASE-HERMES-10-196-83-75.hadoop.jd.local:2181");
            conf.set("zookeeper.znode.parent","/hbase_Hermes");
            //conf.set("hbase.zookeeper.quorum","panda3:2181,panda4:2181,panda5:2181");
           // conf.set("zookeeper.znode.parent","/hbase_lions");
            conf.set("hbase.client.retries.number","10");
           // Connection connection = HConnectionManager.createConnection(conf);
            UserProvider userProvider = new UserProvider();
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hadp");
            User user = userProvider.create(ugi);

            Connection connection = HConnectionManager.createConnection(conf, user);


            Job job = Job.getInstance(conf, "HFile Generator");
            job.setJarByClass(TestBulkLoad.class);
            // set mapper class
            job.setMapperClass(TestHFileMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);
            //job.setNumReduceTasks(20);
            // set input and output path
            String inputPath = "/test/test.txt";
            String outputPath = "/test/hfiles";
            logger.info("in:{},out:{}",inputPath,outputPath);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            logger.info("-----add in and out-----");
            // other config
            HFileOutputFormat2.configureIncrementalLoad(job,connection.getTable(TableName.valueOf("test_bulk")),connection.getRegionLocator(TableName.valueOf("test_bulk")));
            // begin
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
            boolean res = job.waitForCompletion(true);
            logger.info("result:{}",res);
            LoadIncrementalHFiles loadFiles = new LoadIncrementalHFiles(conf);
            loadFiles.doBulkLoad(new Path(outputPath), (HTable) connection.getTable(TableName.valueOf("test_bulk")));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
