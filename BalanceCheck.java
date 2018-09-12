import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.http.Consts;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by huangteng on 2018/5/2.
 */
public class BalanceCheck {
    private static Logger logger = LoggerFactory.getLogger(BalanceCheck.class);

    public static void main(String[] args) {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(5);
        service.scheduleAtFixedRate(new Balance(),1,3, TimeUnit.SECONDS);
    }
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
}

class Balance implements Runnable {

    private static Logger logger1 = LoggerFactory.getLogger(Balance.class);

    public void run() {
        logger1.info("开始获取各集群表region的写请求量");

        List<NameValuePair> paramPut = Lists.newArrayList();
        paramPut.add(new BasicNameValuePair("start","15m-ago"));
        paramPut.add(new BasicNameValuePair("end","5m-ago"));
        paramPut.add(new BasicNameValuePair("m", "sum:rate:2m-avg:hbase.regionserver.regions.mutateCount{hbase=*,table=*}"));

        String result = BalanceCheck.getdata(paramPut);

        if(null != result){
            JSONArray jsonArray = JSON.parseArray(result);
            logger1.info("此次查询Put共获取{}组数据！", String.valueOf(jsonArray.size()));
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jb = (JSONObject) jsonArray.get(i);
                String hbaseName = String.valueOf(((JSONObject) jb.get("tags")).get("cluster"));
                if ("venus_major".equals(hbaseName))
                    hbaseName = "venus";
                if ("vesta_major".equals(hbaseName))
                    hbaseName = "vesta";
                String table = String.valueOf(((JSONObject) jb.get("tags")).get("table"));
                String namespace = String.valueOf(((JSONObject) jb.get("tags")).get("namespace"));
                String hbaseValue = String.valueOf(jb.get("dps"));
                JSONObject jsonObject = JSON.parseObject(hbaseValue);
                List<Integer> tablePut = new ArrayList<Integer>();
                //获取表ops数
                for(JSONObject.Entry<String,Object> entry:jsonObject.entrySet()){
                    tablePut.add(Integer.parseInt(String.valueOf(entry.getValue())));
                }
                if(tablePut.size()>1){
                    if(containNeg(tablePut))
                        continue;

                }


            }
        }
    }

    private String getParam(String hbase,String table,String region){
        return new StringBuilder().append("sum:rate:2m-avg:hbase.regionserver.regions.mutateCount{hbase=").append(hbase).append(",table=")
                .append(table).append(",region=").append(region).append("}").toString();
    }

    private boolean containNeg(List<Integer> list){
        for (int i : list) {
            if (i < 0)
                return true;
        }
        return false;
    }
}
