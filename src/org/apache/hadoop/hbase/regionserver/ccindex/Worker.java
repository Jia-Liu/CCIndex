package org.apache.hadoop.hbase.regionserver.ccindex;
//package org.apache.hadoop.hbase.regionserver.tableindexed;
//
//import java.io.IOException;
//import java.lang.reflect.InvocationTargetException;
//import java.lang.reflect.Method;
//import java.util.SortedMap;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReentrantLock;
//
//import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
//
//public class Worker extends Thread {
//
//	private ThreadPool pool;
//	private int workerNum;
//	private Object[] args;
//	private boolean stoped=false;
//	public void toStop()
//	{
//		this.stoped=true;
//	}
//	public Worker(int i, ThreadPool p) {
//		this.workerNum = i;
//		this.pool = p;
//		this.setName("worker " + i);
//		
//	}
//public void setNull()
//{
//	this.pool=null;
//	this.args=null;
//}
//	public void setArgs(Object[] args) {
//		this.args = args;
//	}
//
//	public void doWork() {
//		try {
//			// System.out.println(pool);
//			// System.out.println(pool.getFather());
//			// System.out.println(args);
//			
//				try {
//					if(args.length==4)
//					{
//						IndexedRegion.updateIndexMultTable(((IndexedRegion)args[0]),
//								(IndexSpecification) args[1], (byte[]) args[2],
//								(SortedMap<byte[], byte[]>) args[3],null);
//					
//					}
//					else if(args.length==5)
//					{
//								IndexedRegion.updateIndexMultTable(((IndexedRegion)args[0]),
//								(IndexSpecification) args[1], (byte[]) args[2],
//								(SortedMap<byte[], byte[]>) args[3],(Integer)args[4]);
//					}
//					this.args=null;
//					
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
////			}
////			else
////			{
////				try {
////					pool.getM().invoke(pool.getFather(), args);
////				} catch (IllegalAccessException e) {
////					// TODO Auto-generated catch block
////					e.printStackTrace();
////				} catch (InvocationTargetException e) {
////					// TODO Auto-generated catch block
////					e.printStackTrace();
////				}
////			}
//		} catch (IllegalArgumentException e) {
//			// TODO Auto-generated catch block
//			//System.err.println(this.workerNum+ "in throw clause illegalArgument");
//			e.printStackTrace();
//		}
//		// catch (IllegalAccessException e) {
//		// // TODO Auto-generated catch block
//		// System.err.println(this.workerNum+"in throw clause illegalAccessE");
//		// e.printStackTrace();
//		// } catch (InvocationTargetException e) {
//		// // TODO Auto-generated catch block
//		// System.err.println(this.workerNum+"in throw clause InvocationTarg");
//		// e.printStackTrace();
//		// }
//	}
//	public boolean shouldStop()
//	{
//		if(this.stoped)
//		{
//			this.setNull();
//			return true;
//		}
//		else
//		{
//			return false;
//		}
//	}
//	@SuppressWarnings("finally")
//	public void run() {
//		boolean first = true;
//
//		while (true) {
//			if(shouldStop())
//			{
//				return;
//				
//			}
//			// System.err.println(this.workerNum+"begain to do work!");
//			if (first) {
//				synchronized (this) {
//					try {
//						if(shouldStop())
//						{
//							return;
//						}
//						wait();
//						first = false;
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				}
//			}
//			if(shouldStop())
//				return;
//			try {
//				if(!shouldStop())
//					this.doWork();
//			} catch (Throwable e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//				throw e;
//			} finally {
//				//synchronized (pool.lock) {
//					synchronized (this) {
//						try {
//							if (pool.lastTestAndAdd()) 
//							{
//								this.pool.notifySelf();
//								if(shouldStop())
//								{return;}
//								
//							}
//							if(shouldStop())
//								{return;}
//							if (!first) {
//								this.wait();
//							}
//						} catch (InterruptedException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//					}
//			//	}
//				continue;
//			}
//		}
//	}
//}
