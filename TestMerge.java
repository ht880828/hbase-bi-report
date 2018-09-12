import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by huangteng on 2018/5/23.
 */
//集群表region合并工具类
public class TestMerge {

    private static Logger logger = LoggerFactory.getLogger(TestMerge.class);

    public static void main(String[] args) throws IOException {
        String zkIp = args[0];
        String zNode = args[1];
        String tableNameList = args[2];
        String size = args[3];
        int tableSize = Integer.parseInt(size);

        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum",zkIp);//"panda3:2181"
        configuration.set("zookeeper.znode.parent",zNode);//"/hbase_lions"

        UserProvider userProvider = new UserProvider();
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hadp");
        User user = userProvider.create(ugi);

        Connection connection = HConnectionManager.createConnection(configuration, user);

        Admin admin = connection.getAdmin();

        for(String tableName:tableNameList.split(",")) {
            RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(tableName));
            RegionSizeCalculator regionSizeCalculator = new RegionSizeCalculator(regionLocator, admin);

            List<HRegionInfo> hRegionInfoList = admin.getTableRegions(TableName.valueOf(tableName));
            logger.info("---表" + tableName + "共有" + String.valueOf(hRegionInfoList.size()) + "个region！---");
            int numMerge = 0;
            if (hRegionInfoList.size() > 1) {
                for (int i = 0; i < hRegionInfoList.size(); i++) {
                    if (i == hRegionInfoList.size() - 1)
                        break;
                    long regionSize = regionSizeCalculator.getRegionSize(hRegionInfoList.get(i).getRegionName());
                    long regionSize_next = regionSizeCalculator.getRegionSize(hRegionInfoList.get(i + 1).getRegionName());
                    if (regionSize < tableSize*1024 && regionSize_next < tableSize*1024) {
                        // logger.info("----" + hRegionInfoList.get(i).getEncodedName() + ":" + Long.toString(regionSize));
                        admin.mergeRegions(hRegionInfoList.get(i).getEncodedNameAsBytes(), hRegionInfoList.get(i + 1).getEncodedNameAsBytes(), false);
                        i++;
                        numMerge++;
                        logger.info("---"+ tableName +"---Done " + String.valueOf(numMerge) + "th merge operation!---");
                    }
                }
            }
            logger.info("---"+ tableName +"---finished merge_region,complete " + String.valueOf(numMerge) + " times merge_region!---");
        }
        admin.close();
        connection.close();
    }
}
