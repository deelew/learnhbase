package kxq.dilu.hbase.definitive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: dilu.kxq
 * Date: 14-5-19
 * Time: ����11:49
 * To change this template use File | Settings | File Templates.
 */
public class PutExample {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.125.4.243");
        HTable table = new HTable(config, "testtable");
        Bytes Bytes;
        Put put = new Put("row1".getBytes());
        put.add("colfam1".getBytes(), "colA".getBytes(), "value3".getBytes());
        table.put(put);
        System.out.println("done! Good job");
    }

    @Test
    public void test2() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, "10.232.31.216,10.232.31.215,10.232.31.219");
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "3325");
//        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase_94_htc");
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor[] htds = admin.listTables();
//        for (HTableDescriptor htd : htds) {
//            System.out.println(htd.getNameAsString());
//        }

        HTable table = new HTable(conf, "eagleeye_access");
        Map<HRegionInfo, ServerName> locations = table.getRegionLocations();
        byte[] family = "family1".getBytes();
        ResultScanner scanner = table.getScanner(family);
        Result record = null;
        String[] columns = new String[]{ "refer", "pv", "cStatus", "status", "rt"};
        while ((record = scanner.next()) != null) {
            System.out.println("===");
            System.out.print(Bytes.toString(record.getRow()));
            System.out.print(":"  );
            for (String column : columns) {
                System.out.print(",\t" + column +"=");
                System.out.print(record.getColumn(family, column.getBytes()));
            }
        }
//        System.out.println(locations.size());
        HConnectionManager.deleteConnection(conf, true);
    }
}
