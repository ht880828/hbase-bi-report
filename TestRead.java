import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * Created by huangteng on 2017/5/5.
 */
public class TestRead {
    public static void main(String[] args) {
        try {
            System.out.println("---111---");
            Configuration configuration = new Configuration();
            configuration.set("bdp.hbase.erp", "huangteng1");//���erp
            configuration.set("bdp.hbase.instance.name", "SL1000000003029");//SL1000000003029//SL1000000003036
            configuration.set("bdp.hbase.accesskey", "MZYH5UIKEY3BUB2QI3KHPIBGTM");//MZYH5UIKEY3BUB2QI3KHPIBGTM//MZYH5UIKEY3BUKN66MEHMAGILM
//            configuration.set("hbase.client.operation.timeout","300000");
//            configuration.set("hbase.rpc.timeout","200000");
//            configuration.set("hbase.client.scanner.timeout.period","200000");
            //configuration.set("bdp.hbase.host", "192.168.178.95,192.168.178.96,192.168.178.97,192.168.178.98");
            // configuration.set("conf.switch.zookeeper.quorum", "172.20.249.91,172.20.249.92,172.20.249.93,172.20.249.94,172.20.249.95");//���ϻ������ã�����֧��һ���л��Ӽ�Ⱥ��һ���л����ӳ�0~1������ɣ�ʹ��ǰ���hbase��ȷ�������һ���л���ʼ�����á�

            Connection connection = ConnectionFactory.createConnection(configuration);//���ֵ���
            Table table = connection.getTable(TableName.valueOf("test:TestTable"));//test:TestTable//bi:copyqueuedata

            Put put = new Put(Bytes.toBytes(String.valueOf(1)));
            put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("age"), Bytes.toBytes("16"));
            table.put(put);
            System.out.println("---888---");
//            Get get = new Get(Bytes.toBytes("100000"));
//
//            if (table.exists(get)) {
//                System.out.println(table.get(get).toString());
//                Cell[] cell = table.get(get).rawCells();
//                System.out.println(Bytes.toString(table.get(get).getValue(Bytes.toBytes("t"),Bytes.toBytes("age"))));
//                for(int i=0;i<cell.length;i++){
//                    System.out.println(Bytes.toString(cell[i].getRow()));
//                    System.out.println(Bytes.toString(cell[i].getFamily()));
//                    System.out.println(Bytes.toString(cell[i].getQualifier()));
//                    System.out.println(Bytes.toString(cell[i].getValue()));
//                }
//            }
//            else
//                System.out.println("not exist!");
            // Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryPrefixComparator(Bytes.toBytes(3)));
            //SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("cf_1"),Bytes.toBytes("coloum_test1"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("value_test1") );
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("data"));
            // scan.setFilter(singleColumnValueFilter);
//            scan.setStartRow(Bytes.toBytes(3));
//            scan.setStopRow(Bytes.toBytes(4));
            // scan.setFilter(filter);
//            scan.setTimeRange(1,2);
//            scan.addFamily(Bytes.toBytes("t"));
//            scan.setCaching(10);
            //scan.setCaching(100000);
            System.out.println("---start---");
            ResultScanner resultScanner = table.getScanner(scan);
            System.out.println("---222---");
            int i = 0;
            for (Result r : resultScanner) {
                if (i < 10)
//                System.out.println(r.getRow().toString());
//                i++;
                    System.out.println(Bytes.toString(r.getRow()) + " " + Bytes.toString(r.getValue(Bytes.toBytes("d"), Bytes.toBytes("data"))));
            }
            //  System.out.println(i);
        } catch (Exception e) {
            System.out.println("~~~444~~~");
            e.printStackTrace();
        } finally {
            System.out.println("over");
        }
    }
}
