import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.Consts;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Object;
import java.io.IOException;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by huangteng on 2018/2/2.
 */
public class ClusterSlaAdjust {

    private static Logger logger = LoggerFactory.getLogger(ClusterSlaAdjust.class);

    public static void main(String[] args) {
        try {
            logger.info("因不可抗拒原因，开始调整sla可用率数据");
            String cluster = args[0];
            String group = args[1];
            String time = args[2];
            String putsla = args[3];
            String getsla = args[4];

            List<Put> slaList = new LinkedList<Put>();

            for(String gp:group.split(",")){
                Put put = new Put(Bytes.toBytes(cluster+"|"+gp+"|"+time));
                put.addColumn(Bytes.toBytes("d"),Bytes.toBytes("putsla"),Bytes.toBytes(putsla));
                put.addColumn(Bytes.toBytes("d"),Bytes.toBytes("getsla"),Bytes.toBytes(getsla));
                slaList.add(put);
            }

            Configuration configuration = new Configuration();
            configuration.set("bdp.hbase.erp", "huangteng1");//erp
            configuration.set("bdp.hbase.instance.name", "SL1000000003036");//实例-SL1000000002865
            configuration.set("bdp.hbase.accesskey", "MZYH5UIKEY3BUKN66MEHMAGILM");//AccessKey-MZYH5UIKEY3BUD3ZU3XVFEZROA
            Connection connection = ConnectionFactory.createConnection(configuration);//保持单例

            logger.info("成功连接HBase，开始更新sla数据");

            Table table = connection.getTable(TableName.valueOf("bi:sladata"));//test:test_hbase_bdp
            table.put(slaList);
            logger.info("sla数据更新成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
