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
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by huangteng on 2018/5/6.
 */
//自动检查可用率小于1的集群分组，并调整为1
public class ClusterAutoSla {
    private static Logger logger = LoggerFactory.getLogger(ClusterAutoSla.class);

    public static String getdata(List<NameValuePair> params) {
        try {
            HttpClient httpClient = new HttpClient();
            String str = EntityUtils.toString(new UrlEncodedFormEntity(params, Consts.UTF_8));
            GetMethod method = new GetMethod("http://172.19.185.87:4242/api/query?" + str);
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
            logger.info("开始执行获取hbase的sla指标任务");
            String starttime = args[0];
            String endtime = args[1];
            logger.info("starttime:{},endtime:{}", starttime, endtime);
            BasicNameValuePair start = new BasicNameValuePair("start", starttime);
            BasicNameValuePair end = new BasicNameValuePair("end", endtime);

            //<editor-fold desc="sla指标">
            //目前仅有1.1.6的sla数据
            //PutSla
            List<NameValuePair> paramsPutSla = Lists.newArrayList();

            //GetSla
            List<NameValuePair> paramsGetSla = Lists.newArrayList();

            paramsPutSla.add(start);
            paramsPutSla.add(end);
            paramsPutSla.add(new BasicNameValuePair("m", "sum:1m-avg:ump.Put.rate{cluster=*,group=*}"));

            paramsGetSla.add(start);
            paramsGetSla.add(end);
            paramsGetSla.add(new BasicNameValuePair("m", "sum:1m-avg:ump.Get.rate{cluster=*,group=*}"));

            //</editor-fold>

            //the array need to adjust
            List<String> needToAdjust = new ArrayList<String>();

//            NumberFormat nf = NumberFormat.getInstance();

            //<editor-fold desc = "PutSla逻辑">
            String resPutSla = getdata(paramsPutSla);
            if (null != resPutSla) {
                JSONArray jsonArray = JSON.parseArray(resPutSla);
                logger.info("此次查询PutSla共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("cluster"));
                    if("venus_major".equals(hbaseName))
                        hbaseName = "venus";
                    if("vesta_major".equals(hbaseName))
                        hbaseName = "vesta";
                    try {
                        String groupName = String.valueOf(((JSONObject) jb.get("tags")).get("group"));
                        String hbaseValue = String.valueOf(jb.get("dps"));
                        JSONObject jsonObject = JSON.parseObject(hbaseValue);
                        Map<String, String> map = new HashMap<String, String>();
                        Map<String,Float> treeMap = new TreeMap<String, Float>();
                        List<Float> data1 = new ArrayList<Float>();
                        for (JSONObject.Entry<String, Object> entry : jsonObject.entrySet()) {
                            String timestempt = entry.getKey();
                            String date = new SimpleDateFormat("yyyy-MM-dd").format(Long.parseLong(timestempt) * 1000);
                            map.put("time", date);
                            treeMap.put(timestempt, Float.parseFloat(String.valueOf(entry.getValue())));
                        }
                        for(Map.Entry<String,Float> x:treeMap.entrySet()){
                            data1.add(x.getValue());
                        }
                        getsla(map, data1, "put");
                        if(Float.parseFloat(map.get("putsla")) < 1.0){
                            logger.info("---putsla下降---hbasename:{}---hbasegroup:{}---",hbaseName,groupName);
                            needToAdjust.add(hbaseName + "," + groupName);
                        }
                    } catch (Exception e) {
                        logger.info("---putsla---hbasename:{}---hbasevalue:{}---",hbaseName,jb.toJSONString());
                        e.printStackTrace();
                    }
                }
            }
            //</editor-fold>

            //<editor-fold desc = "GetSla逻辑">
            String resGetSla = getdata(paramsGetSla);
            if (null != resGetSla) {
                JSONArray jsonArray = JSON.parseArray(resGetSla);
                logger.info("此次查询GetSla共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("cluster"));
                    if("venus_major".equals(hbaseName))
                        hbaseName = "venus";
                    if("vesta_major".equals(hbaseName))
                        hbaseName = "vesta";
                    try {
                        String groupName = String.valueOf(((JSONObject) jb.get("tags")).get("group"));
                        String hbaseValue = String.valueOf(jb.get("dps"));

                        JSONObject jsonObject = JSON.parseObject(hbaseValue);
                        Map<String, String> map = new HashMap<String, String>();
                        Map<String,Float> treeMap = new TreeMap<String, Float>();
                        List<Float> data1 = new ArrayList<Float>();
                        for (JSONObject.Entry<String, Object> entry : jsonObject.entrySet()) {
                            String timestempt = entry.getKey();
                            String date = new SimpleDateFormat("yyyy-MM-dd").format(Long.parseLong(timestempt) * 1000);
                            map.put("time", date);
                            treeMap.put(timestempt, Float.parseFloat(String.valueOf(entry.getValue())));
                        }
                        for(Map.Entry<String,Float> x:treeMap.entrySet()){
                            data1.add(x.getValue());
                        }
                        getsla(map, data1, "get");
                        if(Float.parseFloat(map.get("getsla")) < 1.0){
                            logger.info("---getsla下降---hbasename:{}---hbasegroup:{}---",hbaseName,groupName);
                            needToAdjust.add(hbaseName + "," + groupName);
                        }
                    } catch (Exception e) {
                        logger.info("---getsla---hbasename:{}---hbasevalue:{}---",hbaseName,jb.toJSONString());
                        e.printStackTrace();
                    }
                }
            }
            //</editor-fold>

            logger.info("---needtoadjust:{}---",needToAdjust);
            if(needToAdjust.size()>0){
                logger.info("---开始调整sla---");
                Calendar calendar = Calendar.getInstance();
                calendar.add(Calendar.DATE,-1);
                String adjustDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
                String adjustSla = "1.0";

                List<Put> slaNew = new LinkedList<Put>();

                for(String cluster:needToAdjust){
                    Put put = new Put(Bytes.toBytes(cluster.split(",")[0] + "|"+cluster.split(",")[1] + "|"+adjustDate));
                    put.addColumn(Bytes.toBytes("d"),Bytes.toBytes("putsla"),Bytes.toBytes(adjustSla));
                    put.addColumn(Bytes.toBytes("d"),Bytes.toBytes("getsla"),Bytes.toBytes(adjustSla));
                    slaNew.add(put);
                }

                Configuration configuration = new Configuration();
                configuration.set("bdp.hbase.erp", "huangteng1");//erp
                configuration.set("bdp.hbase.instance.name", "SL1000000003036");//实例-SL1000000002865
                configuration.set("bdp.hbase.accesskey", "MZYH5UIKEY3BUKN66MEHMAGILM");//AccessKey-MZYH5UIKEY3BUD3ZU3XVFEZROA
                Connection connection = ConnectionFactory.createConnection(configuration);//保持单例

                logger.info("成功连接HBase，开始更新sla数据");

                Table table = connection.getTable(TableName.valueOf("bi:sladata"));//test:test_hbase_bdp
                table.put(slaNew);
                logger.info("sla数据更新成功");
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private static void getsla(Map<String, String> map, List<Float> data,String action) {
        int low = 0;
        int need = 0;
        for (float i:data){
            if(i<100)
                low++;
            else{
                if(low<5)
                    low=0;
                else {
                    need += low;
                    low = 0;
                }
            }
        }
        float result = (float)Math.round((data.size()-need)*10000/data.size())/10000;
        map.put(action + "sla", String.valueOf(result));
    }
}
