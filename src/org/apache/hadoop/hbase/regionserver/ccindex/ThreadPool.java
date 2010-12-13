package org.apache.hadoop.hbase.regionserver.ccindex;
//package org.apache.hadoop.hbase.regionserver.tableindexed;
//
//import java.util.HashMap;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReentrantLock;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//public class ThreadPool{
//	private static final Log LOG = LogFactory.getLog(ThreadPool.class);
//	public int threadNum;
//	private int clockLock=0;
//	public final Object lock=new Object();
//	private long lastRun=0L;
//	private Lock threadLock = new ReentrantLock();
//	long startTime=System.currentTimeMillis();
//
//
//	public boolean shouldRestart()
//	{
////		if(System.currentTimeMillis()-this.startTime>300000)
////			return true;
////		else
////			return false;
//		return false;
//	}
//	public HashMap<Integer,Worker> threadMap=new HashMap<Integer,Worker>();
//	public ThreadPool(int threadNum,long startTime)
//	{
//
//		this.threadNum=threadNum;
//		for(int i=0;i<this.threadNum;i++)
//		{
//
//			this.threadMap.put(i, new Worker(i,this));
//
//		}
//		this.startTime=startTime;
//
//		init();
//	}
//
//	public void init()
//	{
//
//		for(int i=0;i<this.threadNum;i++)
//		{
//			
//			this.threadMap.get(i).start();
//
//		}
//		this.setClock();
//		try {
//			Thread.currentThread().sleep(1000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//	
//	public void setClock()
//	{
//		this.clockLock=0;
//	}
//	public synchronized boolean lastTestAndAdd()
//	{
//		
//		return (++this.clockLock)==this.threadNum;
//	}
//
//	public void notifyAllWorker()
//	{
//		for(int i:this.threadMap.keySet())
//		{
//			synchronized(this.threadMap.get(i))
//			{
//				//System.out.println("notify "+i+"worker");
//				this.threadMap.get(i).notifyAll();
//			}
//			
//		}
//	}
//	@SuppressWarnings("deprecation")
//	public void stopAll()
//	{
//
//		for(int i:this.threadMap.keySet())
//		{
//		
//				//System.out.println("notify "+i+"worker");
//				this.threadMap.get(i).toStop();
//				
//		}
//		this.notifyAllWorker();
//
//		//this.run();
//	}
//
//	public boolean shouldDestroy()
//	{
//		if(this.lastRun!=0&& (System.currentTimeMillis()-this.lastRun>180000))
//		{
//			return true;
//		}
//		return false;
//	}
//	public void run() {
//		
//		// TODO Auto-generated method stub
//		this.lastRun=System.currentTimeMillis();
//		if(this.threadNum==0)
//			return;
//			
//			this.setClock();
//			synchronized (this.lock) 
//			{
//				
//				//System.out.println("pool begain to notify all !");
//				this.notifyAllWorker();
//			//	System.out.println("pool begain to notify all  over!");
//				try {
//				//	System.out.println(Thread.currentThread().getName());
//					//System.out.println("pool begain to sleep!");
//					this.lock.wait();
//					//System.out.println("pool begain to sleep over!");
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//				//	System.err.println("pool in throw clause IntergerrruptedE");
//					e.printStackTrace();
//				}
//			}
//			
//		
//	}
//	public void lockPool()
//	{
//		//System.out.println(Thread.currentThread().getId()+"!!!!!!!!!!!!!!!!lock pool");
//		this.threadLock.lock();
//		
//	}
//	public void unlockPool()
//	{
//	//	System.out.println(Thread.currentThread().getId()+"!!!!!!!!!!!!!!!!unlock pool");
//		this.threadLock.unlock();
//	}
//	public void notifySelf()
//	{
//		synchronized (this.lock) 
//		{
//			this.lock.notifyAll();
//		}
//	}
//	public void setArgs(int i,Object[] arg)
//	{
//
//		this.threadMap.get(i).setArgs(arg);
//	}
//}
