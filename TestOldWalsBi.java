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

import java.io.IOException;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by huangteng on 2018/5/17.
 */
//oldwals指标数据采集，目前尚未集成到JA中
public class TestOldWalsBi {
    private static Logger logger = LoggerFactory.getLogger(TestOldWalsBi.class);

    public static String getdata(List<NameValuePair> params) {
        try {
            HttpClient httpClient = new HttpClient();
            String str = EntityUtils.toString(new UrlEncodedFormEntity(params, Consts.UTF_8));
            GetMethod method = new GetMethod("http://172.19.185.91:4242/api/query?" + str);
            method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                    new DefaultHttpMethodRetryHandler(3, false));
            logger.info(method.getURI().toString());
            int j = httpClient.executeMethod(method);
            if (200 != j) {
                logger.info(String.valueOf(j));
                return null;
            }
            return method.getResponseBodyAsString();
        } catch (IOException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        try {
            logger.info("开始执行获取hbase存储指标任务");
            String starttime = args[0];
            String endtime = args[1];
            logger.info("starttime:{},endtime:{}", starttime, endtime);
            BasicNameValuePair start = new BasicNameValuePair("start", starttime);
            BasicNameValuePair end = new BasicNameValuePair("end", endtime);
            NumberFormat nf = NumberFormat.getInstance();

            //<editor-fold desc="OldWalsSize指标">
            List<NameValuePair> paramzk = Lists.newArrayList();
            paramzk.add(start);
            paramzk.add(end);
            paramzk.add(new BasicNameValuePair("m", "max:2m-avg:hbase.master.wal.oldWALsSize{hbase=*}"));
            //</editor-fold>

            //OldWalsSize-putlist
            List<Put> putListzk = new LinkedList<Put>();

            //<editor-fold desc="OldWalsSize逻辑">
            String reszk = getdata(paramzk);
            if (null != reszk) {
                JSONArray jsonArray = JSON.parseArray(reszk);
                logger.info("此次查询oldWalsSize共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("hbase"));
                    if ("venus_major".equals(hbaseName))
                        hbaseName = "venus";
                    if ("vesta_major".equals(hbaseName))
                        hbaseName = "vesta";
                    String hbaseValue = String.valueOf(jb.get("dps"));
                    JSONObject jsonObject = JSON.parseObject(hbaseValue);
                    Map<String, String> map = new HashMap<String, String>();
                    try {
                        for (JSONObject.Entry<String, Object> entry : jsonObject.entrySet()) {
                            String timestempt = entry.getKey();
                            String date = new SimpleDateFormat("yyyy-MM-dd").format(Long.parseLong(timestempt) * 1000);
                            map.put("time", date);
                            //单位是G
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))/1024/1024/1024).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes("oldwals|" + hbaseName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListzk.add(put);
                    }catch(Exception e){
                        logger.info("---oldwals---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                    }
                }
            }
            logger.info("putListzk大小size为:" + String.valueOf(putListzk.size()));
            //</editor-fold>
            Configuration configuration = new Configuration();
            configuration.set("bdp.hbase.erp", "huangteng1");//erp
            configuration.set("bdp.hbase.instance.name", "SL1000000003036");//实例-SL1000000002865
            configuration.set("bdp.hbase.accesskey", "MZYH5UIKEY3BUKN66MEHMAGILM");//AccessKey-MZYH5UIKEY3BUD3ZU3XVFEZROA
            Connection connection = ConnectionFactory.createConnection(configuration);//保持单例

            logger.info("成功连接HBase，开始插入数据");

            Table tablezk = connection.getTable(TableName.valueOf("bi:oldwaldata"));
            tablezk.put(putListzk);
            logger.info("oldwals数据插入成功");
        }catch(Exception e){
            e.printStackTrace();
        }


    }
}
