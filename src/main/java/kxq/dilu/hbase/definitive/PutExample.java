package kxq.dilu.hbase.definitive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: dilu.kxq
 * Date: 14-5-19
 * Time: ÉÏÎç11:49
 * To change this template use File | Settings | File Templates.
 */
public class PutExample {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","10.125.4.243");
        HTable table = new HTable(config, "testtable");
        Bytes Bytes;
        Put put = new Put("row1".getBytes());
        put.add("colfam1".getBytes(), "colA".getBytes(),"value2".getBytes());
        table.put(put);

    }
    static void test2( ) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, "db152013.sqa.cm6.tbsite.net,db152012.sqa.cm6.tbsite.net,db152015.sqa.cm6.tbsite.net");
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "3525");
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase_94_htc");
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor[]  htds = admin.listTables();
        for (HTableDescriptor htd : htds) {
            System.out.println(htd.getNameAsString());
        }

        HTable table = new HTable(conf, "BuyerList");
        Map<HRegionInfo   ,ServerName> locations = table.getRegionLocations();
        System.out.println(locations.size());
        HConnectionManager.deleteConnection(conf, true);
    }
}
