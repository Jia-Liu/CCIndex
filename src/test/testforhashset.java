package test;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.hbase.util.Bytes;

public class testforhashset {
	public static void main(String [] args)
	{
		HashSet<byte[]> s=new HashSet<byte[]>();
		HashMap<byte[],byte[]> m=new HashMap<byte[],byte[]>();
		s.add(Bytes.toBytes("abcd"));
		System.out.println(new String(Bytes.toBytes("abcd"))+Bytes.toBytes("abcd"));
		System.out.println(new String(Bytes.toBytes("abcd"))+Bytes.toBytes("abcd"));
		System.out.println(new String(Bytes.toBytes("abcd"))+Bytes.toBytes("abcd"));
		System.out.println(new String(Bytes.toBytes("abcd"))+Bytes.toBytes("abcd"));
		m.put(Bytes.toBytes("abcd"),Bytes.toBytes("abcd"));
		System.out.println(s.contains(Bytes.toBytes("abcd"))+""+m.get(Bytes.toBytes("abcd")));
		
	}

}
