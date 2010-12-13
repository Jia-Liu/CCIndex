package org.apache.hadoop.hbase.regionserver.ccindex;

import java.io.IOException;
import java.util.TimerTask;

public class Flusher extends TimerTask{

	
	

	IndexedRegion r;
	Flusher(IndexedRegion r)
	{
		this.r=r;
	}
	public void run() 
	{
		// TODO Auto-generated method stub
		try {
			if(r!=null)
			r.flushcache();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
