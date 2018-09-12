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
    //集群其他重要metric指标获取后存储在adam集群
public class TestMain4 {

    private static Logger logger = LoggerFactory.getLogger(TestMain4.class);

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

            //<editor-fold desc="zk指标">
            List<NameValuePair> paramzk = Lists.newArrayList();
            paramzk.add(start);
            paramzk.add(end);
            paramzk.add(new BasicNameValuePair("m", "sum:2m-avg:zookeeper.zk_open_file_descriptor_count{hadoop=*,host=*}"));
            //</editor-fold>

            //<editor-fold desc="负载指标">
            List<NameValuePair> paramload = Lists.newArrayList();
            paramload.add(start);
            paramload.add(end);
            paramload.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.master.server.averageLoad{hbase=*}"));

            List<NameValuePair> paramload94 = Lists.newArrayList();
            paramload94.add(start);
            paramload94.add(end);
            paramload94.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.master.AverageLoad{hbase=*}"));
            //</editor-fold>

            //<editor-fold desc="regioncount指标">
            List<NameValuePair> paramregionclu = Lists.newArrayList();
            paramregionclu.add(start);
            paramregionclu.add(end);
            paramregionclu.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.server.regionCount{hbase=*}"));

            List<NameValuePair> paramregionhost = Lists.newArrayList();
            paramregionhost.add(start);
            paramregionhost.add(end);
            paramregionhost.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.server.regionCount{hbase=*,host=*}"));

            List<NameValuePair> paramregionclu94 = Lists.newArrayList();
            paramregionclu94.add(start);
            paramregionclu94.add(end);
            paramregionclu94.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.regionserverstatistics.regions{hbase=*}"));

            List<NameValuePair> paramregionhost94 = Lists.newArrayList();
            paramregionhost94.add(start);
            paramregionhost94.add(end);
            paramregionhost94.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.regionserverstatistics.regions{hbase=*,host=*}"));
            //</editor-fold>

            //<editor-fold desc="deadregioncount指标">
            List<NameValuePair> paramdead = Lists.newArrayList();
            paramdead.add(start);
            paramdead.add(end);
            paramdead.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.master.server.numDeadRegionServers{hbase=*}"));

            List<NameValuePair> paramdead94 = Lists.newArrayList();
            paramdead94.add(start);
            paramdead94.add(end);
            paramdead94.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.master.DeadRegionServers{hbase=*}"));
            //</editor-fold>

            //<editor-fold desc="log指标">
            List<NameValuePair> paramlogrserror = Lists.newArrayList();
            paramlogrserror.add(start);
            paramlogrserror.add(end);
            paramlogrserror.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.logs.ERROR{hbase=*,host=*}"));
            List<NameValuePair> paramlogrswarn = Lists.newArrayList();
            paramlogrswarn.add(start);
            paramlogrswarn.add(end);
            paramlogrswarn.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.logs.WARN{hbase=*,host=*}"));
            List<NameValuePair> paramlogmaerror = Lists.newArrayList();
            paramlogmaerror.add(start);
            paramlogmaerror.add(end);
            paramlogmaerror.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.master.logs.ERROR{hbase=*,host=*}"));
            List<NameValuePair> paramlogmawarn = Lists.newArrayList();
            paramlogmawarn.add(start);
            paramlogmawarn.add(end);
            paramlogmawarn.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.master.logs.WARN{hbase=*,host=*}"));
            //</editor-fold>

            //<editor-fold desc="flush指标">
            List<NameValuePair> paramflush = Lists.newArrayList();
            paramflush.add(start);
            paramflush.add(end);
            paramflush.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.server.flushQueueLength{hbase=*,host=*}"));

            List<NameValuePair> paramflush94 = Lists.newArrayList();
            paramflush94.add(start);
            paramflush94.add(end);
            paramflush94.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.regionserverstatistics.flushQueueSize{hbase=*,host=*}"));
            //</editor-fold>

            //<editor-fold desc="compaction指标">
            List<NameValuePair> paramcompaction = Lists.newArrayList();
            paramcompaction.add(start);
            paramcompaction.add(end);
            paramcompaction.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.server.compactionQueueLength{hbase=*,host=*}"));

            List<NameValuePair> paramcompaction94 = Lists.newArrayList();
            paramcompaction94.add(start);
            paramcompaction94.add(end);
            paramcompaction94.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.regionserverstatistics.compactionQueueSize{hbase=*,host=*}"));
            //</editor-fold>

            //<editor-fold desc="copy指标">
            List<NameValuePair> paramcopy = Lists.newArrayList();
            paramcopy.add(start);
            paramcopy.add(end);
            paramcopy.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.replication.source.sizeOfLogQueue{hbase=*,host=*}"));

            List<NameValuePair> paramcopy94 = Lists.newArrayList();
            paramcopy94.add(start);
            paramcopy94.add(end);
            paramcopy94.add(new BasicNameValuePair("m", "sum:2m-avg:hbase.regionserver.replicationsource_for_1.sizeOfLogQueue{hbase=*,host=*}"));
            //</editor-fold>

            //zk-putlist
            List<Put> putListzk = new LinkedList<Put>();
            //load-putlist
            List<Put> putListload = new LinkedList<Put>();
            //regioncount-putlist
            List<Put> putListregioncount = new LinkedList<Put>();
            //deadregions-putlist
            List<Put> putListdeadregions = new LinkedList<Put>();
            //log-putlist
            List<Put> putListlog = new LinkedList<Put>();
            //flushqueue-putlist
            List<Put> putListflushqueue = new LinkedList<Put>();
            //compacqueue-putlist
            List<Put> putListcompactqueue = new LinkedList<Put>();
            //copyqueue-putlist
            List<Put> putListcopyqueue = new LinkedList<Put>();

            NumberFormat nf = NumberFormat.getInstance();

            //<editor-fold desc="zk连接数逻辑">
            String reszk = getdata(paramzk);
            if (null != reszk) {
                JSONArray jsonArray = JSON.parseArray(reszk);
                logger.info("此次查询zk连接数共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("hadoop"));
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
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
                        Put put = new Put(Bytes.toBytes("zk|" + hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListzk.add(put);
                    }catch(Exception e){
                        logger.info("---zk---map:{}---hbasename:{}---hostname:{}---hbasevalue:{}---",map,hbaseName,hostName,hbaseValue);
                    }
                }
            }
            logger.info("putListzk大小size为:" + String.valueOf(putListzk.size()));
            //</editor-fold>

            //<editor-fold desc="负载逻辑">
            String resload = getdata(paramload);
            if (null != resload) {
                JSONArray jsonArray = JSON.parseArray(resload);
                logger.info("此次负载查询按1.1.X维度共获取{}组数据！", String.valueOf(jsonArray.size()));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListload.add(put);
                    } catch (Exception e) {
                        logger.info("---load---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }
            String resload94 = getdata(paramload94);
            if (null != resload94) {
                JSONArray jsonArray = JSON.parseArray(resload94);
                logger.info("此次负载查询按0.94维度共获取{}组数据！", String.valueOf(jsonArray.size()));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListload.add(put);
                    }catch(Exception e){
                        logger.info("---load---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                    }
                }
            }
            logger.info("putListload大小size为:" + String.valueOf(putListload.size()));
            //</editor-fold>

            //<editor-fold desc="regioncount逻辑">
            String resregionclu = getdata(paramregionclu);
            if (null != resregionclu) {
                JSONArray jsonArray = JSON.parseArray(resregionclu);
                logger.info("此次regioncount查询按1.1.X集群维度共获取{}组数据！", String.valueOf(jsonArray.size()));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes("cluster|" + hbaseName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes("empty"));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListregioncount.add(put);
                    } catch (Exception e) {
                        logger.info("---regioncount---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }
            String resregionhost = getdata(paramregionhost);
            if (null != resregionhost) {
                JSONArray jsonArray = JSON.parseArray(resregionhost);
                logger.info("此次regioncount查询按1.1.X-host维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            if(null==map.get("data")||"".equals(map.get("data")))
                                continue;
                            break;
                        }
                        Put put = new Put(Bytes.toBytes("host|" + hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListregioncount.add(put);
                    } catch (Exception e) {
                        logger.info("---regioncount---cluster:{},host:{},data:{}---",hbaseName,hostName,map.get("data"));
                    }
                }
            }
            String resregionclu94 = getdata(paramregionclu94);
            if (null != resregionclu94) {
                JSONArray jsonArray = JSON.parseArray(resregionclu94);
                logger.info("此次regioncount查询按0.94集群维度共获取{}组数据！", String.valueOf(jsonArray.size()));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes("cluster|" + hbaseName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes("empty"));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListregioncount.add(put);
                    }catch(Exception e){
                        logger.info("---regioncount-0.94---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                    }
                }
            }
            String resregionhost94 = getdata(paramregionhost94);
            if (null != resregionhost94) {
                JSONArray jsonArray = JSON.parseArray(resregionhost94);
                logger.info("此次regioncount查询按0.94host维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            if(null==map.get("data")||"".equals(map.get("data")))
                                continue;
                            break;
                        }
                        Put put = new Put(Bytes.toBytes("host|" + hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListregioncount.add(put);
                    }catch(Exception e){
                        logger.info("---regioncount-0.94---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                    }
                }
            }
            logger.info("putListregioncount大小size为:" + String.valueOf(putListregioncount.size()));
            //</editor-fold>

            //<editor-fold desc="deadregioncount逻辑">
            String resdead = getdata(paramdead);
            if (null != resdead) {
                JSONArray jsonArray = JSON.parseArray(resdead);
                logger.info("此次deadregion查询按1.1.X维度共获取{}组数据！", String.valueOf(jsonArray.size()));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListdeadregions.add(put);
                    } catch (Exception e) {
                        logger.info("---deadregion---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }
            String resdead94 = getdata(paramdead94);
            if (null != resdead94) {
                JSONArray jsonArray = JSON.parseArray(resdead94);
                logger.info("此次deadregion查询按0.94维度共获取{}组数据！", String.valueOf(jsonArray.size()));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListdeadregions.add(put);
                    }catch(Exception e){
                        logger.info("---deadregion-0.94---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }
            logger.info("putListdeadregions大小size为:" + String.valueOf(putListdeadregions.size()));
            //</editor-fold>

            //<editor-fold desc="log逻辑">
            String reslogrserror = getdata(paramlogrserror);
            if (null != reslogrserror) {
                JSONArray jsonArray = JSON.parseArray(reslogrserror);
                logger.info("此次logrserror查询共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("hbase"));
                    if("apollo_major".equals(hbaseName))
                        hbaseName = "apollo";
                    if("orion_major".equals(hbaseName))
                        hbaseName = "orion";
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("errorcount"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListlog.add(put);
                    }catch(Exception e){
                        logger.info("---logerror---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }
            String reslogrswarn = getdata(paramlogrswarn);
            if (null != reslogrswarn) {
                JSONArray jsonArray = JSON.parseArray(reslogrswarn);
                logger.info("此次logrswarn查询共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("hbase"));
                    if("apollo_major".equals(hbaseName))
                        hbaseName = "apollo";
                    if("orion_major".equals(hbaseName))
                        hbaseName = "orion";
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("warncount"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListlog.add(put);
                    }catch(Exception e){
                        logger.info("---logwarn---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }
            String reslogmaerror = getdata(paramlogmaerror);
            if (null != reslogmaerror) {
                JSONArray jsonArray = JSON.parseArray(reslogmaerror);
                logger.info("此次logmaerror查询共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("hbase"));
                    if("apollo_major".equals(hbaseName))
                        hbaseName = "apollo";
                    if("orion_major".equals(hbaseName))
                        hbaseName = "orion";
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("errorcount"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListlog.add(put);
                    }catch(Exception e){
                        logger.info("---logmaerror---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }
            String reslogmawarn = getdata(paramlogmawarn);
            if (null != reslogmawarn) {
                JSONArray jsonArray = JSON.parseArray(reslogmawarn);
                logger.info("此次logmawarn查询共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
                    String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("hbase"));
                    if("apollo_major".equals(hbaseName))
                        hbaseName = "apollo";
                    if("orion_major".equals(hbaseName))
                        hbaseName = "orion";
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("warncount"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListlog.add(put);
                    }catch(Exception e){
                        logger.info("---logmawarn---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }
            logger.info("putListlog大小size为:" + String.valueOf(putListlog.size()));
            //</editor-fold>

            //<editor-fold desc="flush逻辑">
            String resflush = getdata(paramflush);
            if (null != resflush) {
                JSONArray jsonArray = JSON.parseArray(resflush);
                logger.info("此次flush查询按1.1.X维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListflushqueue.add(put);
                    } catch (Exception e) {
                        logger.info("---flushqueue---map:{}---hbasename:{}---hbasevalue:{}---host:{}---", map, hbaseName, hbaseValue, hostName);
                        e.printStackTrace();
                    }
                }
            }
            String resflush94 = getdata(paramflush94);
            if (null != resflush94) {
                JSONArray jsonArray = JSON.parseArray(resflush94);
                logger.info("此次flush查询按0.94维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListflushqueue.add(put);
                    }catch(Exception e){
                        logger.info("---flushqueue-0.94---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }
            logger.info("putListflush大小size为:" + String.valueOf(putListflushqueue.size()));
            //</editor-fold>

            //<editor-fold desc="compaction逻辑">
            String rescompaction = getdata(paramcompaction);
            if (null != rescompaction) {
                JSONArray jsonArray = JSON.parseArray(rescompaction);
                logger.info("此次compaction查询按1.1.X维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListcompactqueue.add(put);
                    } catch (Exception e) {
                        logger.info("---compaction---map:{}---hbasename:{}---hbasevalue:{}---host:{}---", map, hbaseName, hbaseValue, hostName);
                        e.printStackTrace();
                    }
                }
            }
            String rescompaction94 = getdata(paramcompaction94);
            if (null != rescompaction94) {
                JSONArray jsonArray = JSON.parseArray(rescompaction94);
                logger.info("此次compaction查询按0.94维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListcompactqueue.add(put);
                    }catch(Exception e){
                        logger.info("---compaction-0.94---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }
            logger.info("putListcompaction大小size为:" + String.valueOf(putListcompactqueue.size()));
            //</editor-fold>

            //<editor-fold desc="copy逻辑">
            String rescopy = getdata(paramcopy);
            if (null != rescopy) {
                JSONArray jsonArray = JSON.parseArray(rescopy);
                logger.info("此次copy查询按1.1.X维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListcopyqueue.add(put);
                    } catch (Exception e) {
                        logger.info("---replication---map:{}---hbasename:{}---hbasevalue:{}---host:{}---", map, hbaseName, hbaseValue, hostName);
                        e.printStackTrace();
                    }
                }
            }
            String rescopy94 = getdata(paramcopy94);
            if (null != rescopy94) {
                JSONArray jsonArray = JSON.parseArray(rescopy94);
                logger.info("此次copy查询按0.94维度共获取{}组数据！", String.valueOf(jsonArray.size()));
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jb = (JSONObject) jsonArray.get(i);
                    String hostName = String.valueOf(((JSONObject) jb.get("tags")).get("host"));
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
                            map.put("data", nf.format(Float.parseFloat(String.valueOf(entry.getValue()))).replaceAll(",",""));
                            break;
                        }
                        Put put = new Put(Bytes.toBytes(hbaseName + "|" + hostName + "|" + map.get("time")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"), Bytes.toBytes(map.get("data")));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cluster"), Bytes.toBytes(hbaseName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("host"), Bytes.toBytes(hostName));
                        put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("vdate"), Bytes.toBytes(map.get("time")));
                        putListcopyqueue.add(put);
                    }catch(Exception e){
                        logger.info("---replication-0.94---map:{}---hbasename:{}---hbasevalue:{}---",map,hbaseName,hbaseValue);
                        e.printStackTrace();
                    }
                }
            }
            logger.info("putListcopy大小size为:" + String.valueOf(putListcopyqueue.size()));
            //</editor-fold>

            Configuration configuration = new Configuration();
            configuration.set("bdp.hbase.erp", "huangteng1");//erp
            configuration.set("bdp.hbase.instance.name", "SL1000000003036");//实例-SL1000000002865
            configuration.set("bdp.hbase.accesskey", "MZYH5UIKEY3BUKN66MEHMAGILM");//AccessKey-MZYH5UIKEY3BUD3ZU3XVFEZROA
            Connection connection = ConnectionFactory.createConnection(configuration);//保持单例

            logger.info("成功连接HBase，开始插入数据");

            Table tablezk = connection.getTable(TableName.valueOf("bi:zkdata"));
            tablezk.put(putListzk);
            logger.info("zk数据插入成功");
            Table tableload = connection.getTable(TableName.valueOf("bi:loaddata"));
            tableload.put(putListload);
            logger.info("load数据插入成功");
            Table tableregioncount = connection.getTable(TableName.valueOf("bi:regioncountdata"));
            tableregioncount.put(putListregioncount);
            logger.info("regioncount数据插入成功");
            Table tabledeadregion = connection.getTable(TableName.valueOf("bi:deadregiondata"));
            tabledeadregion.put(putListdeadregions);
            logger.info("deadregioncount数据插入成功");
            Table tablelog = connection.getTable(TableName.valueOf("bi:logdata"));
            tablelog.put(putListlog);
            logger.info("logcount数据插入成功");
            Table tableflush = connection.getTable(TableName.valueOf("bi:flushqueuedata"));
            tableflush.put(putListflushqueue);
            logger.info("flush数据插入成功");
            Table tablecompaction = connection.getTable(TableName.valueOf("bi:compactqueuedata"));
            tablecompaction.put(putListcompactqueue);
            logger.info("compaction数据插入成功");
            Table tablecopy = connection.getTable(TableName.valueOf("bi:copyqueuedata"));
            tablecopy.put(putListcopyqueue);
            logger.info("copy数据插入成功");
        }catch(Exception e){
           e.printStackTrace();
        }


    }
}
