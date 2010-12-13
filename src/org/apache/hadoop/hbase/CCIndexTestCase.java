
package org.apache.hadoop.hbase;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ccindex.IndexKeyGenerator;
import org.apache.hadoop.hbase.client.ccindex.IndexNotFoundException;
import org.apache.hadoop.hbase.client.ccindex.IndexSpecification;
import org.apache.hadoop.hbase.client.ccindex.IndexedTable;
import org.apache.hadoop.hbase.client.ccindex.CCIndexAdmin;
import org.apache.hadoop.hbase.client.ccindex.IndexedTableDescriptor;
import org.apache.hadoop.hbase.client.ccindex.Range;
import org.apache.hadoop.hbase.client.ccindex.ResultReader;
import org.apache.hadoop.hbase.client.ccindex.SimpleIndexKeyGenerator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Script used evaluating of CCIndex,test case use one base table three CCITs and four CCTs.
 * 
 * HBase performance and scalability. Runs a HBase client
 * that steps through one of a set of hardcoded tests or 'experiments' (e.g. a
 * random reads test, a random writes test, etc.). Pass on the command-line
 * which test to run and how many clients are participating in this experiment.

 */
public class CCIndexTestCase  implements HConstants {
	private static final Log LOG = LogFactory
			.getLog(CCIndexTestCase.class.getName()); 
	private static Vector<Long> time=new Vector<Long> ();
	private static final int ROW_LENGTH = 1024;
	private static final int ONE_GB = 1024 * 1000 * 1000; 

	private static final long ROWS_PER_GB = ONE_GB / ROW_LENGTH; 

	private static long times=0;

	static final byte[] COLUMN_FAMILY="family".getBytes();
	protected static HTableDescriptor TABLE_DESCRIPTOR_ORG;
	protected static IndexedTableDescriptor TABLE_DESCRIPTOR;

	protected static IndexSpecification INDEX_SPECIFICATION;
	protected static IndexSpecification INDEX_SPECIFICATION_2;
	protected static IndexSpecification INDEX_SPECIFICATION_3;
	protected static String INDEX_ID = "indexed_INDEX";
	protected static String INDEX_ID_2 = "indexed2_INDEX";
	protected static String INDEX_ID_3 = "indexed3_INDEX";
	public static final byte [] FAMILY_NAME = IndexedTable.INDEX_COL_FAMILY_NAME;
	public static final byte [] FAMILY_NAME_INDEX=IndexedTable.INDEX_COL_FAMILY_NAME;
	public static final byte [] FAMILY_NAME_INDEX_2=IndexedTable.INDEX_COL_FAMILY_NAME;
	public static final byte [] FAMILY_NAME_INDEX_3=IndexedTable.INDEX_COL_FAMILY_NAME;
	
	public static final byte [] QUALIFIER_NAME = Bytes.toBytes("index");
	public static final byte [] QUALIFIER_NAME_DATA = Bytes.toBytes("data");
	public static final byte [] FAMILY_NAME_AND_QUALIFIER_NAME_DATA=Bytes.toBytes("__INDEX__:data");
	
	public static final byte [] FAMILY_NAME_AND_QUALIFIER_NAME=Bytes.toBytes("__INDEX__:index");
	public static final byte [] FAMILY_NAME_AND_QUALIFIER_NAME_2=Bytes.toBytes("__INDEX__:index");
	public static final byte [] FAMILY_NAME_AND_QUALIFIER_NAME_3=Bytes.toBytes("__INDEX__:index");

//	protected static List<Long> RAND_LIST = new ArrayList();

//	protected static List<Long> RAND_NUMBER_LIST = new ArrayList();

	static {
		TABLE_DESCRIPTOR_ORG = new HTableDescriptor("TestTable");
		TABLE_DESCRIPTOR_ORG.addFamily(new HColumnDescriptor(FAMILY_NAME));

		try {
			TABLE_DESCRIPTOR=new IndexedTableDescriptor (TABLE_DESCRIPTOR_ORG );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		INDEX_SPECIFICATION =new IndexSpecification(INDEX_ID,new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME},
//				new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA, FAMILY_NAME_AND_QUALIFIER_NAME, FAMILY_NAME_AND_QUALIFIER_NAME_2, FAMILY_NAME_AND_QUALIFIER_NAME_3},new SimpleIndexKeyGenerator(FAMILY_NAME_AND_QUALIFIER_NAME));
//		INDEX_SPECIFICATION_2 =new IndexSpecification(INDEX_ID_2,new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_2},
//				new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA, FAMILY_NAME_AND_QUALIFIER_NAME, FAMILY_NAME_AND_QUALIFIER_NAME_2, FAMILY_NAME_AND_QUALIFIER_NAME_3},new SimpleIndexKeyGenerator(FAMILY_NAME_AND_QUALIFIER_NAME_2));
//		INDEX_SPECIFICATION_3 =new IndexSpecification(INDEX_ID_3,new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_3},
//				new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA, FAMILY_NAME_AND_QUALIFIER_NAME, FAMILY_NAME_AND_QUALIFIER_NAME_2, FAMILY_NAME_AND_QUALIFIER_NAME_3},new SimpleIndexKeyGenerator(FAMILY_NAME_AND_QUALIFIER_NAME_3));
//	
		INDEX_SPECIFICATION =new IndexSpecification(INDEX_ID,new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME},
				new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA,FAMILY_NAME_AND_QUALIFIER_NAME_2,FAMILY_NAME_AND_QUALIFIER_NAME_3},new SimpleIndexKeyGenerator(FAMILY_NAME_AND_QUALIFIER_NAME),TABLE_DESCRIPTOR_ORG.getNameAsString());
		INDEX_SPECIFICATION_2 =new IndexSpecification(INDEX_ID_2,new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_2},
				new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA,FAMILY_NAME_AND_QUALIFIER_NAME,FAMILY_NAME_AND_QUALIFIER_NAME_3},new SimpleIndexKeyGenerator(FAMILY_NAME_AND_QUALIFIER_NAME_2),TABLE_DESCRIPTOR_ORG.getNameAsString());
		INDEX_SPECIFICATION_3 =new IndexSpecification(INDEX_ID_3,new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_3},
				new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA,FAMILY_NAME_AND_QUALIFIER_NAME_2,FAMILY_NAME_AND_QUALIFIER_NAME},new SimpleIndexKeyGenerator(FAMILY_NAME_AND_QUALIFIER_NAME_3),TABLE_DESCRIPTOR_ORG.getNameAsString());
	
		//INDEX_SPECIFICATION = new IndexSpecification(INDEX_ID,
			//	FAMILY_NAME_AND_QUALIFIER_NAME);
		// INDEX_SPECIFICATION = new IndexSpecification(INDEX_ID,
		// Bytes.toByteArrays(INDEX_COLUMN_NAME),
		// Bytes.toByteArrays(INDEX_COLUMN_NAME),
		// new SimpleIndexKeyGenerator(INDEX_COLUMN_NAME));
		TABLE_DESCRIPTOR.addIndex(INDEX_SPECIFICATION);
		TABLE_DESCRIPTOR.addIndex(INDEX_SPECIFICATION_2);
		TABLE_DESCRIPTOR.addIndex(INDEX_SPECIFICATION_3);
	}
	private static final String ClUSTER_SCAN="clusterScan";
	private static final String RANDOM_READ = "randomRead";

	private static final String RANDOM_READ_MEM = "randomReadMem";

	private static final String RANDOM_WRITE = "randomWrite";

	private static final String SEQUENTIAL_READ = "sequentialRead";

	private static final String SEQUENTIAL_WRITE = "sequentialWrite";

	private static final String SCAN = "scan";
	private static final String SCANORG = "scanorg";
	
	private static final String SCANRANGE = "scanRange";

	private static final List<String> COMMANDS = Arrays.asList(new String[] {
			RANDOM_READ, RANDOM_READ_MEM, RANDOM_WRITE, SEQUENTIAL_READ,
			SEQUENTIAL_WRITE, SCAN ,ClUSTER_SCAN,SCANORG,SCANRANGE});

	volatile HBaseConfiguration conf;

	private boolean miniCluster = false;

	private boolean nomapred = false;

	private int N = 1;

	private long R = ROWS_PER_GB;

	private static final Path PERF_EVAL_DIR = new Path("performance_evaluation");

	/**
	 * Regex to parse lines in input file passed to mapreduce task.
	 */
	public static final Pattern LINE_PATTERN = Pattern
			.compile("startRow=(\\d+),\\s+"
					+ "perClientRunRows=(\\d+),\\s+totalRows=(\\d+),\\s+clients=(\\d+)");

	/**
	 * Enum for map metrics. Keep it out here rather than inside in the Map
	 * inner-class so we can find associated properties.
	 */
	protected static enum Counter {
		/** elapsed time */
		ELAPSED_TIME,
		/** number of rows */
		ROWS
	}

	/**
	 * Constructor
	 * 
	 * @param c
	 *            Configuration object
	 */
	public CCIndexTestCase(final HBaseConfiguration c) {
		this.conf = c;
	}

	/**
	 * Implementations can have their status set.
	 */
	static interface Status {
		/**
		 * Sets status
		 * 
		 * @param msg
		 *            status message
		 * @throws IOException
		 */
		void setStatus(final String msg) throws IOException;
	}

	/**
	 * MapReduce job that runs a performance evaluation client in each map task.
	 */
	@SuppressWarnings("unchecked")


	/*
	 * If table does not already exist, create. @param c Client to use checking.
	 * @return True if we created the table. @throws IOException
	 */
	private boolean checkTable(CCIndexAdmin admin, String cmd)
			throws IOException {
	
		boolean tableExists = admin.tableExists(TABLE_DESCRIPTOR.getBaseTableDescriptor().getName());
		if (tableExists
				&& (cmd.equals(RANDOM_WRITE) || cmd.equals(SEQUENTIAL_WRITE))) {
			try {
				admin.removeIndex(TABLE_DESCRIPTOR.getBaseTableDescriptor().getName(), INDEX_ID);
				admin.removeIndex(TABLE_DESCRIPTOR.getBaseTableDescriptor().getName(), INDEX_ID_2);
				admin.removeIndex(TABLE_DESCRIPTOR.getBaseTableDescriptor().getName(), INDEX_ID_3);
				admin.disableTable("TestTable_CCT");
				admin.disableTable("TestTable_indexed_INDEX_CCT");
				admin.disableTable("TestTable_indexed2_INDEX_CCT");
				admin.disableTable("TestTable_indexed3_INDEX_CCT");
				admin.deleteTable("TestTable_CCT");
				admin.deleteTable("TestTable_indexed_INDEX_CCT");
				admin.deleteTable("TestTable_indexed2_INDEX_CCT");
				admin.deleteTable("TestTable_indexed3_INDEX_CCT");
			} catch (Exception e) {
				e.printStackTrace();
			}
			admin.disableTable(TABLE_DESCRIPTOR.getBaseTableDescriptor().getName());
			admin.deleteTable(TABLE_DESCRIPTOR.getBaseTableDescriptor().getName());
			admin.createIndexedTable(TABLE_DESCRIPTOR);
			LOG.info("Table " + TABLE_DESCRIPTOR + " created");
		} else if (!tableExists) {
			//admin.deleteTable(TABLE_DESCRIPTOR.getBaseTableDescriptor().getName());
			admin.createIndexedTable(TABLE_DESCRIPTOR);
			//admin.createTable(TABLE_DESCRIPTOR);
			LOG.info("Table " + TABLE_DESCRIPTOR + " created");
		}

		return !tableExists;
	}

	/*
	 * We're to run multiple clients concurrently. Setup a mapreduce job. Run
	 * one map per client. Then run a single reduce to sum the elapsed times.
	 * @param cmd Command to run. @throws IOException
	 */


	/*
	 * Run all clients in this vm each to its own thread. @param cmd Command to
	 * run. @throws IOException
	 */
	@SuppressWarnings("unused")
	private void doMultipleClients(final String cmd) throws IOException {
		final List<Thread> threads = new ArrayList<Thread>(this.N);
		final long perClientRows = R / N;

		for (long i = 0; i < this.N; i++) {
			Thread t = new Thread(Long.toString(i)) {
				 
				public void run() {
					super.run();
					CCIndexTestCase pe = new CCIndexTestCase(conf);
					long index = Integer.parseInt(getName());
					try {
						long elapsedTime = pe.runOneClient(cmd, (int)index
								* perClientRows, (int)perClientRows,R,
								new Status() {
									public void setStatus(final String msg)
											throws IOException {
										LOG.info("client-" + getName() + " "
												+ msg);
									}
								});
						LOG.info("Finished " + getName() + " in " + elapsedTime++
								+ "ms writing " + perClientRows + " rows");
						LOG.info("Finished " + getName() + " in " + elapsedTime +
					              "ms writing " + perClientRows + " rows" + 
					              ", throughput " + R * 1000.0 / elapsedTime + 
					              " Rec/s" + "  " + R * ROW_LENGTH / elapsedTime + 
					              " KBytes/s");
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			};
			threads.add(t);
		}
		for (Thread t : threads) {
			t.start();
		}
		for (Thread t : threads) {
			while (t.isAlive()) {
				try {
					t.join();
				} catch (InterruptedException e) {
					LOG.debug("Interrupted, continuing" + e.toString());
				}
			}
		}
	}

	/*
	 * Run a mapreduce job. Run as many maps as asked-for clients. Before we
	 * start up the job, write out an input file with instruction per client
	 * regards which row they are to start on. @param cmd Command to run.
	 * @throws IOException
	 */


	/*
	 * Write input file of offsets-per-client for the mapreduce job. @param c
	 * Configuration @return Directory that contains file written. @throws
	 * IOException
	 */
	private Path writeInputFile(final Configuration c) throws IOException {
		FileSystem fs = FileSystem.get(c);
		if (!fs.exists(PERF_EVAL_DIR)) {
			fs.mkdirs(PERF_EVAL_DIR);
		}
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
		Path subdir = new Path(PERF_EVAL_DIR, formatter.format(new Date()));
		fs.mkdirs(subdir);
		Path inputFile = new Path(subdir, "input.txt");
		PrintStream out = new PrintStream(fs.create(inputFile));
		try {
			for (long i = 0; i < (this.N * 10); i++) {
				// Write out start row, total number of rows per client run:
				// 1/10th of
				// (R/N).
				long perClientRows = (this.R / this.N);
				out.println("startRow=" + i * perClientRows
						+ ", perClientRunRows=" + (perClientRows / 10)
						+ ", totalRows=" + this.R + ", clients=" + this.N);
			}
		} finally {
			out.close();
		}
		return subdir;
	}

	/*
	 * A test. Subclass to particularize what happens per row.
	 */
	static abstract class Test {
		protected final Random rand = new Random(System.currentTimeMillis());
		static long callTimeRW=0L;
		protected final long startRow;

		protected final long perClientRunRows;

		protected final long totalRows;

		protected final Status status;

		protected CCIndexAdmin admin;

		//protected IndexedTable table;
		protected int tableN=5;
		protected IndexedTable tables[];
		protected HTable tableOrg;

		protected volatile HBaseConfiguration conf;

		Test(final HBaseConfiguration conf, final long startRow,
				final long perClientRunRows, final long totalRows,
				final Status status) {
			super();
			this.startRow = startRow;
			this.perClientRunRows = perClientRunRows;
			this.totalRows = totalRows;
			this.status = status;
			//this.table = null;
			this.conf = conf;
		}

		protected String generateStatus(final long sr, final long i,
				final long lr) {
			return sr + "/" + i + "/" + lr;
		}
		static int iflag=0;
		public synchronized IndexedTable getTable()
		{
			iflag++;
			if(iflag>=this.tableN)
			{
				iflag=0;
			}
			
			return this.tables[iflag];
		}
		protected long getReportingPeriod() {
			return this.perClientRunRows / 10;
		}

		void testSetup() throws IOException {
			this.admin = new CCIndexAdmin(conf);
			this.tables=new IndexedTable[this.tableN];
			
			for(int i=0;i<this.tableN;i++)
			{
				this.tables[i]= new IndexedTable(conf, TABLE_DESCRIPTOR.getBaseTableDescriptor().getName());
				this.tables[i].setAutoFlush(false);
				this.tables[i].setWriteBufferSize(1024*1024*10);
				this.tables[i].setScannerCaching(30);
			}
			
		}

		@SuppressWarnings("unused")
		void testTakedown() throws IOException {

			for(int i=0;i<this.tableN;i++)
			{
				this.tables[i].flushCommits();
			}
		}

		/*
		 * Run test @return Elapsed time. @throws IOException
		 */
		long test() throws IOException {
			long elapsedTime;
			testSetup();
			try {
				Thread.currentThread().sleep(4000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long startTime = System.currentTimeMillis();
			try {
				long lastRow = this.startRow + this.perClientRunRows;
				// Report on completion of 1/10th of total.
				System.out.println("!!!!!!!!!!!!!!!!start end are:"+this.startRow+":"+lastRow);
				for (long i = this.startRow; i < lastRow; i++) {
					long start=System.nanoTime();
					testRow(i);
					CCIndexTestCase.time.add(System.nanoTime()-start);
					if (status != null && i > 0
							&& (i % getReportingPeriod()) == 0) {
						status.setStatus(generateStatus(this.startRow, i,
								lastRow));
					}
				}
				elapsedTime = System.currentTimeMillis() - startTime;
			} finally {
				System.out.println("test take down successfully");
				testTakedown();
			}
			return elapsedTime;
		}

		/*
		 * Test for individual row. @param i Row index.
		 */
		abstract void testRow(final long i) throws IOException;

		/*
		 * @return Test name.
		 */
		abstract String getTestName();
	}

	class RandomReadTest extends Test {
		RandomReadTest(final HBaseConfiguration conf, final long startRow,
				final long perClientRunRows, final long totalRows,
				final Status status) {
			super(conf, startRow, perClientRunRows, totalRows, status);
		}

		 
		void testRow(@SuppressWarnings("unused")
		final long i) throws IOException {
			/**
			 * wsc annotate the following origial sentence.
			 * this.table.get(getRandomRow(this.rand, this.totalRows),
			 * COLUMN_NAME);
			 */
			// HTable indexTable = new HTable(conf,
			// Bytes.toString(this.table.getTableName()) + "-" + INDEX_ID);
			// this.table.get(indexTable.get(generateValue(this.rand),
			// Bytes.toBytes("__INDEX__:ROW")).getValue(), COLUMN_NAME);
			byte[] row=getRandomRow(this.rand, this.perClientRunRows,this.startRow);
			Get g=new Get(row);
			g.addColumn(FAMILY_NAME_AND_QUALIFIER_NAME_DATA);
			Result r=this.getTable().get(g);

			//System.out.println(r.toString());
//			ResultScanner scanner =  table.getIndexedScanner(INDEX_ID, getRandomNumber(rand, this.totalRows), null, null, null,Bytes.toByteArrays(FAMILY_NAME_AND_QUALIFIER_NAME));
//			Result rowResult = scanner.next();
//			if (rowResult != null) {
//				rowResult.getRow();
				//rowResult.get(INDEX_COLUMN_NAME);
				//rowResult.get(COLUMN_NAME);
//				LOG.warn("i " + i + " row "
//						+ Bytes.toString(rowResult.getRow()) + " index "
//						+ rowResult.get(INDEX_COLUMN_NAME) + " data "
//						+ new String(rowResult.get(COLUMN_NAME).getValue()));
			//}
		}

		 
		protected long getReportingPeriod() {
			// 
			return this.perClientRunRows / 100;
		}

		 
		String getTestName() {
			return "randomRead";
		}
	}
	
	class RandomWriteTest extends Test {
		
		RandomWriteTest(final HBaseConfiguration conf, final long startRow,
				final long perClientRunRows, final long totalRows,
				final Status status) {
			super(conf, startRow, perClientRunRows, totalRows, status);
		}

		 
		 void testRow(long i) throws IOException {
			 this.callTimeRW++;
			 if(callTimeRW%1000==0)
			 {
				 System.out.println("call times:"+callTimeRW);
			 }
		      byte [] row = getRandomRow(this.rand, this.perClientRunRows,this.startRow);
		      Put put = new Put(row);
		      put.add(FAMILY_NAME_INDEX, QUALIFIER_NAME,format(this.rand.nextLong()));
		      put.add(FAMILY_NAME_INDEX_2, QUALIFIER_NAME,format(this.rand.nextLong()));
		      put.add(FAMILY_NAME_INDEX_3, QUALIFIER_NAME,format(this.rand.nextLong()));
		      put.add(FAMILY_NAME, QUALIFIER_NAME_DATA, generateValue(this.rand));
		      this.getTable().put(put);
		    }


		 
		String getTestName() {
			return "randomWrite";
		}



	}
	/*
	 * Cluster
	 */
	class MultiDimensionRangeQueryTest extends Test{
		MultiDimensionRangeQueryTest(HBaseConfiguration conf, long startRow,
				long perClientRunRows, long totalRows, Status status) {
			super(conf, startRow, perClientRunRows, totalRows, status);
			// TODO Auto-generated constructor stub
		}
		private ResultReader testScanner;
		void testSetup() throws IOException {
			super.testSetup();
			Range[] range= new Range[3];
			Range r1=new Range();
			r1.setBaseColumns(new byte[][]{FAMILY_NAME_AND_QUALIFIER_NAME_DATA});
			r1.setColumn(FAMILY_NAME_AND_QUALIFIER_NAME);
			r1.setStart(format(100));
			r1.setEnd(format(200));
			Range r2=new Range();
			r2.setBaseColumns(new byte[][]{FAMILY_NAME_AND_QUALIFIER_NAME_DATA});
			r2.setColumn(FAMILY_NAME_AND_QUALIFIER_NAME_2);
			r2.setStart(format(100));
			r2.setEnd(format(100));
			Range r3=new Range();
			r3.setBaseColumns(new byte[][]{FAMILY_NAME_AND_QUALIFIER_NAME_DATA});
			r3.setColumn(FAMILY_NAME_AND_QUALIFIER_NAME_3);
			r3.setStart(format(100));
			r3.setEnd(null);
			range[0]=r1;
			range[1]=r2;
			range[2]=r3;
			//c1 like 100<q<=200 and q2==100 and q3>100 
			
			Range[] range2= new Range[3];
			Range r4=new Range();
			r4.setBaseColumns(new byte[][]{FAMILY_NAME_AND_QUALIFIER_NAME_DATA});
			r4.setColumn(FAMILY_NAME_AND_QUALIFIER_NAME);
			r4.setStart(format(2600));
			r4.setEnd(format(5000));
			Range r5=new Range();
			r5.setBaseColumns(new byte[][]{FAMILY_NAME_AND_QUALIFIER_NAME_DATA});
			r5.setColumn(FAMILY_NAME_AND_QUALIFIER_NAME_2);
			r5.setStart(format(2300));
			r5.setEnd(format(24000));
			Range r6=new Range();
			r6.setBaseColumns(new byte[][]{FAMILY_NAME_AND_QUALIFIER_NAME_DATA});
			r6.setColumn(FAMILY_NAME_AND_QUALIFIER_NAME_3);
			r6.setStart(format(300));
			r6.setEnd(null);
			range2[0]=r4;
			range2[1]=r6;
			range2[2]=r5;
			//c2 like 2600<q<=5000 and 2300<q2<=24000 and q3>300 
			
			Range ranges[][]={range,range2};
			// ranges like c1 or c2
			/**
			this.testScanner = table.getScanner(new byte [][] {COLUMN_NAME},
			        format(this.startRow));
			        */
			//this.fillValue();
	
			this.testScanner = this.getTable().MDRQScan(ranges,0);
			this.testScanner.init();
		
		}
		@Override
		String getTestName() {
			// TODO Auto-generated method stub
			return "ClusterScanTest";
		}
		@Override
		void testRow(long i) throws IOException {
			// TODO Auto-generated method stub
			
			Result r=this.testScanner.next();
			if(r!=null)
			System.out.println(r.toString());
			
		}
		
	}
	class ScanRange extends Test 
	{
		private ResultScanner testScanner;
		private ResultScanner testScannerIndexed;
		private String type;
		 ScanRange (final HBaseConfiguration conf, final long startRow,
					final long perClientRunRows, final long totalRows,
					final Status status) {
				super(conf, startRow, perClientRunRows, totalRows, status);
			}
			void testSetup() throws IOException {
				super.testSetup();
				/**
				this.testScanner = table.getScanner(new byte [][] {COLUMN_NAME},
				        format(this.startRow));
				        */
				//this.fillValue();
				byte[] row=format(0);
				Scan scan=new Scan();
				
				scan.addColumn(FAMILY_NAME_AND_QUALIFIER_NAME_DATA);
				this.testScanner = this.getTable().getScanner(scan);

				this.testScannerIndexed= this.getTable().getIndexedScanner(INDEX_ID_2,
						row	,
						null, null,
						null,
						Bytes.toByteArrays(FAMILY_NAME_AND_QUALIFIER_NAME_DATA));
//				this.testScanner = table.getIndexedScanner(INDEX_ID,
//						row	,
//						null, null,
//						null,
//						Bytes.toByteArrays(FAMILY_NAME_AND_QUALIFIER_NAME_DATA));
			}
			void testTakedown() throws IOException {
				if (this.testScanner != null) {
					this.testScanner.close();
					this.testScannerIndexed.close();
				}
				super.testTakedown();
			}
			long test() throws IndexNotFoundException, IOException
			{
				Vector<Integer> interV=new Vector<Integer>();
				Vector<Long> timeOrg=new Vector<Long>();
				Vector<Long> timeIndex=new Vector<Long>();
				Vector<Long> timeCluster=new Vector<Long>();
			
				for(int inter=2;inter<10000;)
				{
					if(inter<1000)
					{
						inter=inter*2;
						
					}
					else
					{
						inter=inter+1000;
					}
					long cTime = 0;
					long oTime = 0;
					long iTime=0;
					int start=0;
					interV.add(inter);
					for(int j=0;j<10;j++)
					{
						start=+inter;
						byte[] row=format((0+start));
						Scan scan=new Scan();
						byte[] rowEnd=format((start+inter+3));
						scan.addColumn(FAMILY_NAME_AND_QUALIFIER_NAME_DATA);
						scan.addColumn(FAMILY_NAME_AND_QUALIFIER_NAME);
						scan.addColumn(FAMILY_NAME_AND_QUALIFIER_NAME_2);
						scan.addColumn(FAMILY_NAME_AND_QUALIFIER_NAME_3);
						scan.setStartRow(row);
						scan.setStopRow(rowEnd);
			
						this.testScanner = this.getTable().getScanner(scan);
	
						this.testScannerIndexed= this.getTable().getIndexedScanner(INDEX_ID_2,
								row	,
								rowEnd, null,
								null,
								Bytes.toByteArrays(FAMILY_NAME_AND_QUALIFIER_NAME_DATA));
						long start1=System.currentTimeMillis();
						for(int k=0;k<inter;k++)
						{
							this.testScanner.next();
						}
						oTime+=(System.currentTimeMillis()-start1);
						start1=System.currentTimeMillis();
						for(int k=0;k<inter;k++)
						{
							this.testScannerIndexed.next();
						}
						cTime+=(System.currentTimeMillis()-start1);
					}
					timeCluster.add(cTime/10);
					
					timeOrg.add(oTime/10);
					System.out.println("interv is:"+inter);
					System.out.println("cTime is:"+(cTime/10));
					System.out.println("oTime is:"+(oTime/10));
					start=0;
					for(int j=0;j<10;j++)
					{
						start=+inter;
						byte[] row=format((0+start));
						byte[] rowEnd=format(start+inter+3);
				 
	
						this.testScannerIndexed= this.getTable().getIndexedScanner(INDEX_ID_2,
								row	, 
								rowEnd, null,
								null,
								Bytes.toByteArrays(FAMILY_NAME_AND_QUALIFIER_NAME_DATA));
						long start1=System.currentTimeMillis();
						start1=System.currentTimeMillis();
						for(int k=0;k<inter;k++)
						{ 
							
							this.testScannerIndexed.nextOrg(1);
						}
						iTime+=(System.currentTimeMillis()-start1);
					}
					timeIndex.add(iTime/10);
					System.out.println("interv is:"+inter);
					System.out.println("iTime is:"+(iTime/10));
				}
				
				for(int i=0;i<interV.size();i++)
				{
					System.out.print("\ninter is:\t "+interV.get(i)+"\t");
					System.out.print("timeO is:\t "+timeOrg.get(i)+"\t");
					System.out.print("timeI is:\t "+ timeIndex.get(i)+"\t");
					System.out.print("timeC is:\t "+ timeCluster.get(i)+"\t\n");
				}
				//System.out.println("oTime is:"+oTime/10);
				return perClientRunRows;
				
			}
			void testRow(@SuppressWarnings("unused")
					final long i) throws IOException {
						
						
						Result r=null;
						r=testScanner.next();
						if(r!=null)
						{
							
						}
					//	if(r!=null)
						//System.out.println(r.toString());
						//else
							//System.out.println(r);
						//r.getValue(family, qualifier);
						
					}

					 
					String getTestName() {
						return "scan";
					}
	}
	class ScanTestOrg extends Test {
		private ResultScanner testScanner;
	    void fillValue() {
	    	long startTime = System.currentTimeMillis();

	    	long lastRow = this.startRow + this.perClientRunRows;
	    	// Report on completion of 1/10th of total.
	    	for (long i = this.startRow; i < lastRow; i++) {
	    		byte[] row = getRandomRow(this.rand, this.totalRows);
	    		Put put = new Put(row);
	    		put.add(FAMILY_NAME_INDEX, QUALIFIER_NAME,format(this.rand.nextLong()));
			      put.add(FAMILY_NAME_INDEX_2, QUALIFIER_NAME,format(this.rand.nextLong()));
			      put.add(FAMILY_NAME_INDEX_3, QUALIFIER_NAME,format(this.rand.nextLong()));
			      put.add(FAMILY_NAME, QUALIFIER_NAME_DATA, generateValue(this.rand));
	    		try {
	    			this.getTable().put(put);
	    			if(i%1000==0)
	    			LOG.debug("#############################put :"+i);
	    		} catch (IOException e) {
	    			// TODO Auto-generated catch block
	    			e.printStackTrace();
	    		}
	    	}
 
	    }
	    ScanTestOrg (final HBaseConfiguration conf, final long startRow,
				final long perClientRunRows, final long totalRows,
				final Status status) {
			super(conf, startRow, perClientRunRows, totalRows, status);
		}

		 
		void testSetup() throws IOException {
			super.testSetup();
			/**
			this.testScanner = table.getScanner(new byte [][] {COLUMN_NAME},
			        format(this.startRow));
			        */
			//this.fillValue();
			byte[] row=Bytes.toBytes("0");
			Scan scan=new Scan();
			scan.addColumn(FAMILY_NAME_AND_QUALIFIER_NAME_DATA);
			scan.addColumn(FAMILY_NAME_AND_QUALIFIER_NAME);
			scan.addColumn(FAMILY_NAME_AND_QUALIFIER_NAME_2);
			scan.addColumn(FAMILY_NAME_AND_QUALIFIER_NAME_3);
			this.testScanner = this.getTable().getScanner(scan);
//			this.testScanner = table.getIndexedScanner(INDEX_ID,
//					row	,
//					null, null,
//					null,
//					Bytes.toByteArrays(FAMILY_NAME_AND_QUALIFIER_NAME_DATA));
		}

		 
		void testTakedown() throws IOException {
			if (this.testScanner != null) {
				this.testScanner.close();
			}
			super.testTakedown();
		}

		 
		void testRow(@SuppressWarnings("unused")
		final long i) throws IOException {
			
			
			Result r=null;
			r=testScanner.next();
			if(r!=null)
			{
				
			}
		//	if(r!=null)
			//System.out.println(r.toString());
			//else
				//System.out.println(r);
			//r.getValue(family, qualifier);
			
		}

		 
		String getTestName() {
			return "scan";
		}
	}
	class ScanTest extends Test {
		private ResultScanner testScanner;
	    void fillValue() {
	    	long startTime = System.currentTimeMillis();

	    	long lastRow = this.startRow + this.perClientRunRows;
	    	// Report on completion of 1/10th of total.
	    	for (long i = this.startRow; i < lastRow; i++) {
	    		byte[] row = getRandomRow(this.rand, this.totalRows);
	    		Put put = new Put(row);
	    		put.add(FAMILY_NAME_INDEX, QUALIFIER_NAME,format(this.rand.nextLong()));
			      put.add(FAMILY_NAME_INDEX_2, QUALIFIER_NAME,format(this.rand.nextLong()));
			      put.add(FAMILY_NAME_INDEX_3, QUALIFIER_NAME,format(this.rand.nextLong()));
			      put.add(FAMILY_NAME, QUALIFIER_NAME_DATA, generateValue(this.rand));
	    		try {
	    			this.getTable().put(put);
	    			if(i%1000==0)
	    			LOG.debug("#############################put :"+i);
	    		} catch (IOException e) {
	    			// TODO Auto-generated catch block
	    			e.printStackTrace();
	    		}
	    	}
 
	    }
		ScanTest(final HBaseConfiguration conf, final long startRow,
				final long perClientRunRows, final long totalRows,
				final Status status) {
			super(conf, startRow, perClientRunRows, totalRows, status);
		}

		 
		void testSetup() throws IOException {
			super.testSetup();
			/**
			this.testScanner = table.getScanner(new byte [][] {COLUMN_NAME},
			        format(this.startRow));
			        */
			//this.fillValue();
			byte[] row=Bytes.toBytes("0");
			this.testScanner = this.getTable().getIndexedScanner(INDEX_ID,
					row	,
					null, null,
					null,
					Bytes.toByteArrays(FAMILY_NAME_AND_QUALIFIER_NAME_DATA));
		}

		 
		void testTakedown() throws IOException {
			if (this.testScanner != null) {
				this.testScanner.close();
			}
			super.testTakedown();
		}

		 
		void testRow(@SuppressWarnings("unused")
		final long i) throws IOException {
			
			
			Result r=null;
			r=testScanner.next();
			if(r!=null)
			{
				
			}
		//	if(r!=null)
			//System.out.println(r.toString());
			//else
				//System.out.println(r);
			//r.getValue(family, qualifier);
			
		}

		 
		String getTestName() {
			return "scan";
		}
	}

	class SequentialReadTest extends Test {
		SequentialReadTest(final HBaseConfiguration conf, final long startRow,
				final long perClientRunRows, final long totalRows,
				final Status status) {
			super(conf, startRow, perClientRunRows, totalRows, status);
		}

		 
		void testRow(final long i) throws IOException {
			// do nothing.
			Get g=new Get(format(i));
			g.addColumn(FAMILY_NAME_AND_QUALIFIER_NAME_DATA);
			 Result r=this.getTable().get(g);
			 //System.out.println(r.toString());
		}

		 
		String getTestName() {
			return "sequentialRead";
		}

		/*
		 * Run test @return Elapsed time. @throws IOException
		 */
//		long test() throws IOException {
//			long elapsedTime;
//			testSetup();
//			long startTime = System.currentTimeMillis();
//			try {
//				long lastRow = this.startRow + this.perClientRunRows;
//				// Report on completion of 1/10th of total.
//				
//
//				for (long i = this.startRow; i < lastRow; i++) {
//					ResultScanner scanner = table.getIndexedScanner(INDEX_ID,
//							format(i), null, null, null, Bytes.toByteArrays(FAMILY_NAME_AND_QUALIFIER_NAME_DATA));
//					Result rowResult = scanner.next();
//					rowResult.getRow();
//					//rowResult.get(INDEX_COLUMN_NAME);
//					//rowResult.get(COLUMN_NAME);
//					// LOG.warn("row " + Bytes.toString(rowResult.getRow()) + "
//					// index " + rowResult.get(INDEX_COLUMN_NAME) + " data " +
//					// new String(rowResult.get(COLUMN_NAME).getValue()));
//					// don't use this.
//					// testRow(Bytes.toInt(rowResult.getRow()));
//					if (status != null && i > 0
//							&& (i % getReportingPeriod()) == 0) {
//						status.setStatus(generateStatus(this.startRow, i,
//								lastRow));
//					}
//				}
//				elapsedTime = System.currentTimeMillis() - startTime;
//			} finally {
//				testTakedown();
//			}
//			return elapsedTime;
//		}
	}

	class SequentialWriteTest extends Test {
		
		SequentialWriteTest(final HBaseConfiguration conf, final long startRow,
				final long perClientRunRows, final long totalRows,
				final Status status) {
			super(conf, startRow, perClientRunRows, totalRows, status);
		}

		 
		void testRow(final long i) throws IOException {
			
			byte [] row =format(i);
			  Put put = new Put(row);
			  put.add(FAMILY_NAME_INDEX, QUALIFIER_NAME,format(i));
		      put.add(FAMILY_NAME_INDEX_2, QUALIFIER_NAME,format(i));
		      put.add(FAMILY_NAME_INDEX_3, QUALIFIER_NAME,format(i));
		      put.add(FAMILY_NAME, QUALIFIER_NAME_DATA, generateValue(this.rand));
		      this.getTable().put(put);
		
		}

		 
		String getTestName() {
			return "sequentialWrite";
		}
	}

	/*
	 * Format passed integer. @param number @return Returns zero-prefixed
	 * 10-byte wide decimal version of passed number (Does absolute in case
	 * number is negative).
	 */
	public static byte[] format(final long number) {
		byte[] b = new byte[10];
		long d = Math.abs(number);
		for (int i = b.length - 1; i >= 0; i--) {
			b[i] = (byte) ((d % 10) + '0');
			d /= 10;
		}
		return b;
	}

	/*
	 * This method takes some time and is done inline uploading data. For
	 * example, doing the mapfile test, generation of the key and value consumes
	 * about 30% of CPU time. @return Generated random value to insert into a
	 * table cell.
	 */
	static byte[] generateValue(final Random r) {
		byte[] b = new byte[ROW_LENGTH];
		// r.nextBytes(b);
		for (int i = 0; i < b.length; ++i) {
			int ch = r.nextInt() % 26;
			if (ch < 0)
				ch = ch * -1;
			b[i] = (byte) (ch + 65);
		}
		return b;
	}

	byte[] getRandomRow(final Random random, final long totalRows) {

		/**
		 * wsc annotate the origial sentence to acquire totally different rand
		 */
		return format(random.nextInt(Integer.MAX_VALUE) % totalRows);

		/**
		 * cancel unique value speciality
		 * @author wsc
		 * @date 2009-11-25
		 */
		/*Long rand = null;
		while (RAND_LIST.contains(rand = new Long(random
				.nextInt(Integer.MAX_VALUE)
				% (totalRows))))
			;
		RAND_LIST.add(rand);	
		return format(rand);*/

	} 
	byte[] getRandomRow(final Random random, final long totalRows,final long startRow) {

		/**
		 * wsc annotate the origial sentence to acquire totally different rand
		 */
		return format((random.nextInt(Integer.MAX_VALUE) % totalRows)+startRow);

		/**
		 * cancel unique value speciality
		 * @author wsc
		 * @date 2009-11-25
		 */
		/*Long rand = null;
		while (RAND_LIST.contains(rand = new Long(random
				.nextInt(Integer.MAX_VALUE)
				% (totalRows))))
			;
		RAND_LIST.add(rand);	
		return format(rand);*/

	} 

	
	byte[] getRandomNumber(final Random random, final long totalRows) {
		
		/**
		 * cancel unique value speciality
		 * @author wsc
		 * @date 2009-11-25
		 */
		/*Long rand = null;
		// two SETs
		while (RAND_NUMBER_LIST.contains(rand = new Long(random
				.nextInt(Integer.MAX_VALUE)
				% (totalRows))))
			;
		RAND_NUMBER_LIST.add(rand);
		return format(rand);*/
		
		return format(random.nextInt(Integer.MAX_VALUE) % totalRows);
	}
byte[] getRandomNumber(final Random random, final long totalRows,long startRows) {
		
		/**
		 * cancel unique value speciality
		 * @author wsc
		 * @date 2009-11-25
		 */
		/*Long rand = null;
		// two SETs
		while (RAND_NUMBER_LIST.contains(rand = new Long(random
				.nextInt(Integer.MAX_VALUE)
				% (totalRows))))
			;
		RAND_NUMBER_LIST.add(rand);
		return format(rand);*/
		
		return format(random.nextInt(Integer.MAX_VALUE) % totalRows+startRows);
	}

	long runOneClient(final String cmd, final long startRow,
			final long perClientRunRows, final long totalRows,
			final Status status) throws IOException {
		status.setStatus("Start " + cmd + " at offset " + startRow + " for "
				+ perClientRunRows + " rows");
		
		long totalElapsedTime = 0;
		if (cmd.equals(RANDOM_READ)) {
			Test t = new RandomReadTest(this.conf, startRow, perClientRunRows,
					totalRows, status);
			totalElapsedTime = t.test();
		}
		else if(cmd.equals(ClUSTER_SCAN))
		{
			Test t = new MultiDimensionRangeQueryTest(this.conf, startRow, perClientRunRows,
					totalRows, status);
			totalElapsedTime = t.test();
		}
		else if(cmd.equals(SCANORG))
		{
			Test t = new ScanTestOrg(this.conf, startRow, perClientRunRows,
					totalRows, status);
			totalElapsedTime = t.test();
		}
		else if(cmd.equals(SCANRANGE))
		{
			Test t = new ScanRange(this.conf, startRow, perClientRunRows,
					totalRows, status);
			totalElapsedTime = t.test();
		}
		else if (cmd.equals(RANDOM_READ_MEM)) {
			throw new UnsupportedOperationException("Not yet implemented");
		} else if (cmd.equals(RANDOM_WRITE)) {
			Test t = new RandomWriteTest(this.conf, startRow, perClientRunRows,
					totalRows, status);
			totalElapsedTime = t.test();
		} else if (cmd.equals(SCAN)) {
			Test t = new ScanTest(this.conf, startRow, perClientRunRows,
					totalRows, status);
			totalElapsedTime = t.test();
		} else if (cmd.equals(SEQUENTIAL_READ)) {
			Test t = new SequentialReadTest(this.conf, startRow,
					perClientRunRows, totalRows, status);
			totalElapsedTime = t.test();
		} else if (cmd.equals(SEQUENTIAL_WRITE)) {
			Test t = new SequentialWriteTest(this.conf, startRow,
					perClientRunRows, totalRows, status);
			totalElapsedTime = t.test();
		} else {
			new IllegalArgumentException("Invalid command value: " + cmd);
		}
		/**
		status.setStatus("Finished " + cmd + " in " + totalElapsedTime
				+ "ms at offset " + startRow + " for " + perClientRunRows
				+ " rows");
				*/
		status.setStatus("Finished " + cmd + " in " + totalElapsedTime 
				+ "ms at offset " + startRow + " for " + perClientRunRows 
				+ " rows" + " throughput " + perClientRunRows * 1000.0 / (totalElapsedTime+1) 
				+ " Rec/s" + "  " + perClientRunRows * ROW_LENGTH / (totalElapsedTime+1) + " KBytes/s");

		return totalElapsedTime;
	}

	private void runNIsOne(final String cmd) {
		Status status = new Status() {
			@SuppressWarnings("unused")
			public void setStatus(String msg) throws IOException {
				LOG.info(msg);
			}
		};

		CCIndexAdmin admin = null;
		try {

			if(cmd.equals(this.SEQUENTIAL_WRITE)||cmd.equals(this.RANDOM_WRITE))
				   checkTable(new CCIndexAdmin(this.conf),cmd);
			runOneClient(cmd, 0, this.R, this.R, status);
		} catch (Exception e) {
			LOG.error("Failed", e);
		}
	}

	private void runTest(final String cmd) throws IOException {
		if (cmd.equals(RANDOM_READ_MEM)) {
			// For this one test, so all fits in memory, make R smaller (See
			// pg. 9 of BigTable paper).
			R = (this.R / 10) * N;
		}



		try {
			if (N == 1) {
				// If there is only one client and one HRegionServer, we assume
				// nothing
				// has been set up at all.
				runNIsOne(cmd);
			} else {
				// Else, run
				try {
					runNIsMoreThanOne(cmd);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace(); 
				}
			}
		} finally {
			java.io.File d=new java.io.File("/opt/result.txt");
			FileWriter w=new FileWriter(d);
			
			for(Long l:CCIndexTestCase.time)
			//LOG.debug(l+""+"ms");
			w.write(l+""+"ms\n");
		}
	}
	private void runNIsMoreThanOne(final String cmd)
	  throws IOException, InterruptedException, ClassNotFoundException {
			if(cmd.equals(this.SEQUENTIAL_WRITE)||cmd.equals(this.RANDOM_WRITE))
	   checkTable(new CCIndexAdmin(this.conf),cmd);
	    doMultipleClients(cmd);
//	    if (this.nomapred) {
//	      
//	    } else {
//	     // doMapReduce(cmd);
//	    }
	  }
	private void printUsage() {
		printUsage(null);
	}

	private void printUsage(final String message) {
		if (message != null && message.length() > 0) {
			System.err.println(message);
		}
		System.err.println("Usage: java " + this.getClass().getName()
				+ " [--master=HOST:PORT] \\");
		System.err
				.println("  [--miniCluster] [--nomapred] [--rows=ROWS] <command> <nclients>");
		System.err.println();
		System.err.println("Options:");
		System.err.println(" master          Specify host and port of HBase "
				+ "cluster master. If not present,");
		System.err
				.println("                 address is read from configuration");
		System.err
				.println(" miniCluster     Run the test on an HBaseMiniCluster");
		System.err
				.println(" nomapred        Run multiple clients using threads "
						+ "(rather than use mapreduce)");
		System.err
				.println(" rows            Rows each client runs. Default: One million");
		System.err.println();
		System.err.println("Command:");
		System.err.println(" clusterScan  Run cluster table scan read test");
		System.err.println(" randomRead      Run random read test");
		System.err.println(" scanRange      Run random read test");
		System.err.println(" randomReadMem   Run random read test where table "
				+ "is in memory");
		System.err.println(" randomWrite     Run random write test");
		System.err.println(" sequentialRead  Run sequential read test");
		System.err.println(" sequentialWrite Run sequential write test");
		System.err.println(" scan            Run scan test");
		System.err.println();
		System.err.println("Args:");
		System.err
				.println(" nclients        Integer. Required. Total number of "
						+ "clients (and HRegionServers)");
		System.err.println("                 running: 1 <= value <= 500");
		System.err.println("Examples:");
		System.err.println(" To run a single evaluation client:");
		System.err
				.println(" $ bin/hbase "
						+ "org.apache.hadoop.hbase.PerformanceEvaluation sequentialWrite 1");
	}

	private void getArgs(final int start, final String[] args) {
		if (start + 1 > args.length) {
			throw new IllegalArgumentException(
					"must supply the number of clients");
		}

		N = Integer.parseInt(args[start]);
		if (N > 500 || N < 1) {
			throw new IllegalArgumentException(
					"Number of clients must be between " + "1 and 500.");
		}

		// Set total number of rows to write.
		//this.R = this.R * N;
	}

	private int doCommandLine(final String[] args) {
		// Process command-line args. TODO: Better cmd-line processing
		// (but hopefully something not as painful as cli options).
		int errCode = -1;
		if (args.length < 1) {
			printUsage();
			return errCode;
		}

		try {
			for (int i = 0; i < args.length; i++) {
				String cmd = args[i];
				if (cmd.equals("-h") || cmd.startsWith("--h")) {
					printUsage();
					errCode = 0;
					break;
				}

			

				final String miniClusterArgKey = "--miniCluster";
				if (cmd.startsWith(miniClusterArgKey)) {
					this.miniCluster = true;
					continue;
				} 

				final String nmr = "--nomapred";
				if (cmd.startsWith(nmr)) {
					this.nomapred = true;
					continue;
				}

				final String rows = "--rows=";
				if (cmd.startsWith(rows)) {
					this.R = Integer.parseInt(cmd.substring(rows.length()));
					continue;
				}

				if (COMMANDS.contains(cmd)) {
					getArgs(i + 1, args);
					runTest(cmd);
					errCode = 0;
					break;
				}

				printUsage();
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return errCode;
	}

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		HBaseConfiguration c = new HBaseConfiguration();
		System.exit(new CCIndexTestCase(c).doCommandLine(args));
		
	}
}
