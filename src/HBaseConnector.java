import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.RowResult;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
public class HBaseConnector {
	public static void creatTable(String table,HBaseConfiguration conf) throws IOException{
		HConnection conn = HConnectionManager.getConnection(conf);
	    HBaseAdmin admin = new HBaseAdmin(conf);
	    if(!admin.tableExists(table)){
	      System.out.println("1. " + table + " table creating ... please wait");
	      HTableDescriptor tableDesc = new HTableDescriptor(table);
	      tableDesc.addFamily(new HColumnDescriptor("http:"));
	      tableDesc.addFamily(new HColumnDescriptor("url:"));
	      tableDesc.addFamily(new HColumnDescriptor("referrer:"));
	      admin.createTable(tableDesc);

	    } else {
	      System.out.println("1. " + table + " table already exists.");
	    }
	    System.out.println("2. access_log files fetching using map/reduce");
  }

	public static void retrievePost(String postId) throws IOException {
		HBaseConfiguration conf = new HBaseConfiguration();   
		//conf.set("hbase.master","10.61.0.161");   
		
		//conf.set("hbase.server.address","10.61.0.161:60020"); 
		creatTable("blogposts",conf);
		HTable table = new HTable(conf, "blogposts");
		Map post = new HashMap();
	
		Put p=new Put();
		p.add(new KeyValue("http:protocol".getBytes(),"sdfsdf".getBytes()));
		table.put(p);

		
	}

	public static void main(String[] args) throws IOException {
	 HBaseConnector.retrievePost("post1");

	}
}