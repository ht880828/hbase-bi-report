import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Created by huangteng on 2017/5/10.
 */
public class ClusterSwitch {

    private static Logger logger = LoggerFactory.getLogger(ClusterSwitch.class);
    private java.sql.Connection con;
    private java.sql.PreparedStatement ps;

    public ClusterSwitch() {
        try {
            con = DriverManager.getConnection("jdbc:mysql://172.22.178.98:3306/hbase_sniper_online?useUnicode=true&characterEncoding=UTF8", "root", "hadoop");
            // con = DriverManager.getConnection("jdbc:mysql://192.168.178.91:3306/hbase_sniper_online?useUnicode=true&characterEncoding=UTF8", "admin", "123456");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String executeQuery(String sql, String param) {
        try {
            ps = con.prepareStatement(sql);
            if (null != param) {
                ps.setString(1, param);
            }
            ResultSet rs = ps.executeQuery();
            StringBuilder sb = new StringBuilder();
            String ipsource = "";
            String clusource = "";
            while (rs.next()) {
                String ip = rs.getString(2);
                String port = rs.getString(3);
                clusource = rs.getString(4);
                for (String tmp : ip.split(",")) {
                    sb.append(tmp).append(":").append(port).append(",");
                }
                ipsource = sb.substring(0, sb.length() - 1).toString();
            }
            ipsource = ipsource + "," + clusource;
            return ipsource;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void close() {
        if (null != con) {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public int switchclu(ZooKeeper zk, String path, String clusource, String cludest, String ipdest) throws KeeperException, InterruptedException {
        int result = 0;
        Stat stat = new Stat();
        List<String> ins = zk.getChildren(path, true);
        logger.info("---children:{}---parent:{}---", ins.toString(), path);
        for (String tmp : ins) {
            if (!"/".equals(path))
                tmp = "/" + tmp;
            if (zk.getACL(path + tmp, stat).get(0).getId().getScheme().indexOf("world") == -1)
                continue;
            if (null != zk.getChildren(path + tmp, true) && zk.getChildren(path + tmp, true).size() > 0)
                switchclu(zk, path + tmp, clusource, cludest, ipdest);
            String node;
            if (null != zk.getData(path + tmp, true, null)) {
                node = new String(zk.getData(path + tmp, true, null));
                if (node.indexOf(clusource) >= 0 && node.indexOf("zookeeper.znode.parent") >= 0) {
                    logger.info("---node:{}---nodedata:{}---", path + tmp, node);
                    JSONObject jb = JSON.parseObject(node);
                    if (jb.containsKey("zookeeper.znode.parent"))
                        jb.put("zookeeper.znode.parent", cludest);
                    if (jb.containsKey("hbase.zookeeper.quorum"))
                        jb.put("hbase.zookeeper.quorum", ipdest);
                    zk.setData(path + tmp, jb.toJSONString().getBytes(), -1);
                    result++;
                    logger.debug("---nodedata:{}", new String(zk.getData(path + tmp, true, null)));
                }
            }
        }
        return result;
    }

    public int switchins(ZooKeeper zk, String path, String instance, String cludest, String ipdest) throws KeeperException, InterruptedException {
        int result = 0;
        Stat stat = new Stat();
        List<String> ins = zk.getChildren(path, true);
        logger.info("---children:{}---parent:{}---", ins.toString(), path);
        for (String tmp : ins) {
            if (!"/".equals(path))
                tmp = "/" + tmp;
            if (zk.getACL(path + tmp, stat).get(0).getId().getScheme().indexOf("world") == -1)
                continue;
            if (null != zk.getChildren(path + tmp, true) && zk.getChildren(path + tmp, true).size() > 0)
                switchclu(zk, path + tmp, instance, cludest, ipdest);
            String node;
            if (instance.indexOf(tmp) != -1) {
                if (null != zk.getData(path + tmp, true, null)) {
                    node = new String(zk.getData(path + tmp, true, null));
                    logger.info("---node:{}---nodedata:{}---", path + tmp, node);
                    JSONObject jb = JSON.parseObject(node);
                    if (jb.containsKey("zookeeper.znode.parent"))
                        jb.put("zookeeper.znode.parent", cludest);
                    if (jb.containsKey("hbase.zookeeper.quorum"))
                        jb.put("hbase.zookeeper.quorum", ipdest);
                    zk.setData(path + tmp, jb.toJSONString().getBytes(), -1);
                    result++;
                    logger.debug("---nodedata:{}", new String(zk.getData(path + tmp, true, null)));
                }
            }
        }
        return result;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, KeeperException, InterruptedException {
        String ipsource = "";
        String ipdest = "";
        String clusource = "";
        String cludest = "";
        String defaultvalue = "kangaroo";
        String defaultvalue1 = "koala";
        String defaultvalue2 = "172.16.160.25,172.19.141.63,172.19.141.54";
        String defaultvalue3 = "all";
        if (null != args && args.length > 0) {
            defaultvalue = args[0];
            defaultvalue1 = args[1];
            defaultvalue2 = args[2];
            defaultvalue3 = args[3];
        }
        logger.info("---{}-{}-{}-{}---", defaultvalue, defaultvalue1, defaultvalue2,defaultvalue3);
        // Properties pro = new Properties();
        //InputStream fi = new BufferedInputStream(new FileInputStream("src/main/resources/data.properties"));
        //InputStream fi = new BufferedInputStream(new FileInputStream("data.properties"));
        // pro.load(fi);

        ClusterSwitch cs = new ClusterSwitch();
        String rs = cs.executeQuery("select cluster_name,zk_ips,zk_port,znode_parent from t_cluster where cluster_name = ?", defaultvalue);
        String rs1 = cs.executeQuery("select cluster_name,zk_ips,zk_port,znode_parent from t_cluster where cluster_name = ?", defaultvalue1);
        ipsource = rs.substring(0, rs.lastIndexOf(","));
        clusource = rs.substring(rs.lastIndexOf(",") + 1);
        ipdest = rs1.substring(0, rs1.lastIndexOf(","));
        cludest = rs1.substring(rs1.lastIndexOf(",") + 1);
        cs.close();
        logger.info("---ipsource:{},clusource:{},ipdest:{},cludest:{}", ipsource, clusource, ipdest, cludest);
        ZooKeeper zk = new ZooKeeper(defaultvalue2, 6000, new Watcher() {
            // 监听所有触发事件
            public void process(WatchedEvent event) {
                logger.info("已经触发了{}事件！", event.getType());
            }
        });
        int result;
        if ("all".equals(defaultvalue3))
            result = cs.switchclu(zk, "/", clusource, cludest, ipdest);
        else
            result = cs.switchins(zk, "/", defaultvalue3, cludest, ipdest);
        logger.info("---success!---,切换了{}个实例", result);
    }
}
