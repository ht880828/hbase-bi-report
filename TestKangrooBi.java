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
 * Created by huangteng on 2017/5/11.
 */
//预计每天凌晨6点定时跑
    //集群存储指标获取后存储在kangaroo集群(目前已弃用)
public class TestKangrooBi {

    private static Logger logger = LoggerFactory.getLogger(TestKangrooBi.class);

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

            //<editor-fold desc="存储指标">
            //1.1.X的存储指标
            List<NameValuePair> paramsClu = Lists.newArrayList();//集群维度
            List<NameValuePair> paramsTab = Lists.newArrayList();//表维度
            List<NameValuePair> paramsGroup = Lists.newArrayList();//分组维度
            paramsClu.add(start);
            paramsClu.add(end);
            paramsClu.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.server.storeFileSize{hbase=*}"));

            paramsGroup.add(start);
            paramsGroup.add(end);
            paramsGroup.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.server.storeFileSize{hbase=*,group=*}"));

            paramsTab.add(start);
            paramsTab.add(end);
            paramsTab.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.regions.storeFileSize{hbase=*,group=*,table=*}"));

            //0.94的存储指标
            List<NameValuePair> paramsClu94 = Lists.newArrayList();//集群维度
            List<NameValuePair> paramsTab94 = Lists.newArrayList();//表维度
            paramsClu94.add(start);
            paramsClu94.add(end);
            paramsClu94.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.regionserverdynamicstatistics.storeFileSizeMB{hbase=*}"));

            paramsTab94.add(start);
            paramsTab94.add(end);
            paramsTab94.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.regionserverdynamicstatistics.storeFileSizeMB{hbase=*,table=*}"));
            //</editor-fold>

            //存储putlist
            List<Put> putList = new LinkedList<Put>();

            NumberFormat nf = NumberFormat.getInstance();

            //<editor-fold desc="存储逻辑">
            String resClu = getdata(paramsClu);
            if (null != resClu) {
                JSONArray jsonArray = JSON.parseArray(resClu);
                logger.info("此次查询按1.1.X集群维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("hbase"));
                    if("venus_major".equals(hbaseName))
                        hbaseName = "venus";
                    if("vesta_major".equals(hbaseName))
                        hbaseName = "vesta";
                    String hbaseValue = String.valueOf(jb.get("dps"));
                    JSONObject jsonObject = JSON.parseObject(hbaseValue);
                    Map<String, String> map = new HashMap<String, String>();
                    try {
                        for (JSONObject.Entry<String, Object> entry : jsonObject.entrySet()) {
                            String timestempt = entry.getKey();
                            String date = new SimpleDateFormat("yyyy-MM-dd").format(Long.parseLong(timestempt) * 1000);
                            map.put("time", date);
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue())) / 1024 / 1024).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes("cluster|" + hbaseName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("group"), Bytes.toBytes("empty"));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("table"), Bytes.toBytes("empty"));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("date"), Bytes.toBytes(map.get("time")));
                        putList.add(put);
                    } catch (Exception e) {
                        logger.info("--1.1.X---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }

            String resGroup = getdata(paramsGroup);
            if (null != resGroup) {
                JSONArray jsonArray = JSON.parseArray(resGroup);
                logger.info("此次查询按1.1.X分组维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("hbase"));
                    if("venus_major".equals(hbaseName))
                        hbaseName = "venus";
                    if("vesta_major".equals(hbaseName))
                        hbaseName = "vesta";
                    String groupName = String.valueOf(((JSONObject) jb.get("tags")).get("group"));
                    String hbaseValue = String.valueOf(jb.get("dps"));
                    JSONObject jsonObject = JSON.parseObject(hbaseValue);
                    Map<String, String> map = new HashMap<String, String>();
                    try {
                        for (JSONObject.Entry<String, Object> entry : jsonObject.entrySet()) {
                            String timestempt = entry.getKey();
                            String date = new SimpleDateFormat("yyyy-MM-dd").format(Long.parseLong(timestempt) * 1000);
                            map.put("time", date);
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue())) / 1024 / 1024).replaceAll(",", ""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes("group|" + hbaseName + "|" + groupName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("group"), Bytes.toBytes(groupName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("table"), Bytes.toBytes("empty"));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("date"), Bytes.toBytes(map.get("time")));
                        putList.add(put);
                    } catch (Exception e) {
                        logger.info("---1.1.X---map:{}---hbasename:{}---groupname:{}---hbasevalue:{}---",map,hbaseName,groupName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }

            String resTab = getdata(paramsTab);
            if (null != resTab) {
                JSONArray jsonArray = JSON.parseArray(resTab);
                logger.info("此次查询按1.1.X表维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("hbase"));
                    if("venus_major".equals(hbaseName))
                        hbaseName = "venus";
                    if("vesta_major".equals(hbaseName))
                        hbaseName = "vesta";
                    String groupName = String.valueOf(((JSONObject) jb.get("tags")).get("group"));
                    String tableName = String.valueOf(((JSONObject) jb.get("tags")).get("table"));
                    String hbaseValue = String.valueOf(jb.get("dps"));
                    JSONObject jsonObject = JSON.parseObject(hbaseValue);
                    Map<String, String> map = new HashMap<String, String>();
                    try {
                        for (JSONObject.Entry<String, Object> entry : jsonObject.entrySet()) {
                            String timestempt = entry.getKey();
                            String date = new SimpleDateFormat("yyyy-MM-dd").format(Long.parseLong(timestempt) * 1000);
                            map.put("time", date);
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue())) / 1024 / 1024).replaceAll(",", ""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes("table|" + hbaseName + "|" + groupName + "|" + tableName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("group"), Bytes.toBytes(groupName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("table"), Bytes.toBytes(tableName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("date"), Bytes.toBytes(map.get("time")));
                        putList.add(put);
                    }catch(Exception e){
                        logger.info("---1.1.X---map:{}---hbasename:{}---groupname:{}---tablename:{}---hbasevalue:{}---",map,hbaseName,groupName,tableName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }

            String resClu94 = getdata(paramsClu94);
            if (null != resClu94) {
                JSONArray jsonArray = JSON.parseArray(resClu94);
                logger.info("此次查询按0.94集群维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("hbase"));
                    if("apollo_major".equals(hbaseName))
                        hbaseName = "apollo";
                    if("orion_major".equals(hbaseName))
                        hbaseName = "orion";
                    String hbaseValue = String.valueOf(jb.get("dps"));
                    JSONObject jsonObject = JSON.parseObject(hbaseValue);
                    Map<String, String> map = new HashMap<String, String>();
                    try {
                        for (JSONObject.Entry<String, Object> entry : jsonObject.entrySet()) {
                            String timestempt = entry.getKey();
                            String date = new SimpleDateFormat("yyyy-MM-dd").format(Long.parseLong(timestempt) * 1000);
                            map.put("time", date);
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",", ""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes("cluster|" + hbaseName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("group"), Bytes.toBytes("empty"));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("table"), Bytes.toBytes("empty"));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("date"), Bytes.toBytes(map.get("time")));
                        putList.add(put);
                    } catch (Exception e) {
                        logger.info("---0.94---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }

            String resTab94 = getdata(paramsTab94);
            if (null != resTab94) {
                JSONArray jsonArray = JSON.parseArray(resTab94);
                logger.info("此次查询按0.94表维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("hbase"));
                    if("apollo_major".equals(hbaseName))
                        hbaseName = "apollo";
                    if("orion_major".equals(hbaseName))
                        hbaseName = "orion";
                    String tableName = String.valueOf(((JSONObject) jb.get("tags")).get("table"));
                    String hbaseValue = String.valueOf(jb.get("dps"));
                    JSONObject jsonObject = JSON.parseObject(hbaseValue);
                    Map<String, String> map = new HashMap<String, String>();
                    try {
                        for (JSONObject.Entry<String, Object> entry : jsonObject.entrySet()) {
                            String timestempt = entry.getKey();
                            String date = new SimpleDateFormat("yyyy-MM-dd").format(Long.parseLong(timestempt) * 1000);
                            map.put("time", date);
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes("table|" + hbaseName + "|default|" + tableName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("group"), Bytes.toBytes("default"));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("table"), Bytes.toBytes(tableName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("date"), Bytes.toBytes(map.get("time")));
                        putList.add(put);
                    }catch(Exception e){
                        logger.info("---0.94---map:{}---hbasename:{}---tablename:{}---hbasevalue:{}---",map,hbaseName,tableName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }
            logger.info("putList大小size为:" + String.valueOf(putList.size()));
            //</editor-fold>

            Configuration configuration = new Configuration();
            configuration.set("bdp.hbase.erp", "huangteng1");//erp
            configuration.set("bdp.hbase.instance.name", "SL1000000002865");//实例-SL1000000002865
            configuration.set("bdp.hbase.accesskey", "MZYH5UIKEY3BUD3ZU3XVFEZROA");//AccessKey-MZYH5UIKEY3BUD3ZU3XVFEZROA
            configuration.set("hbase.client.retries.number","5");
            Connection connection = ConnectionFactory.createConnection(configuration);//保持单例

            logger.info("成功连接HBase，开始插入数据");

            Table table = connection.getTable(TableName.valueOf("test:test_hbase_bdp"));//test:test_hbase_bdp
            table.put(putList);
            logger.info("存储数据插入成功");

        }catch(Exception e){
            e.printStackTrace();
        }


    }
}
