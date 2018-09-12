import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by huangteng on 2017/7/26.
 */
public class TestHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private static final byte[] FAMILY_BYTE = Bytes.toBytes("f");
    private static final byte[] QUALIFIER_INDEX = Bytes.toBytes("f");
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 得到rowKey以及value，并转成字节
        String line = value.toString();
        String[] items = line.split(" ", -1);
        String rownum = items[0];
        String colval = items[1];
        byte[] rownumBytes = Bytes.toBytes(rownum);
        byte[] colvalBytes = Bytes.toBytes(colval);
        // 生成Put对象
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rownumBytes);
        Put put = new Put(rowKey.copyBytes());
        put.addColumn(FAMILY_BYTE,QUALIFIER_INDEX,colvalBytes); // 测试的时候value和key相同
        // 输出
        context.write(rowKey, put);
    }
}
