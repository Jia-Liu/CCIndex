package org.apache.hadoop.hbase.client.ccindex;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * 
 * @author liu jia liujia09@software.ict.ac.cn
 *
 */
public class SingleReader {
	private ResultScanner scanner;
	private Range[] range;
	private int rowN=100;
	private boolean over=false;
	public SingleReader(ResultScanner scanner, Range[] range)
	{
		this.scanner=scanner;
		this.range=range;
	}

	public Vector<Result> next() throws IOException
	{
		Result[] rs=null;
	
		Vector<Result>ret=new Vector<Result>();
		while((rs=scanner.next(rowN))!=null&&rs.length!=0)
		{
			
			for(Result r:rs)
			{
				boolean discard=false;
				for(Range one:range)
				{
					byte [] value=r.getValue(one.getColumn());

					if(value==null)
					{
						discard=true;
						break;
					}
					if(one.getStart()!=null)
					{
						if(Bytes.compareTo(value, one.getStart())<0)
						{
							discard=true;
							break;
						}
					}
					if(one.getEnd()!=null)
					{
						if(Bytes.compareTo(one.getEnd(), value)<0)
						{
							discard=true;
							break;
						}
					}
				}
				if(!discard)
					ret.add(r);
			}
			
			return ret;
			
		}
		this.over=true;
		return ret;
		
	}
	public boolean isOver()
	{
		return this.over;
	}

}
