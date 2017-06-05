package triangleJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements  
Reducer<Text, Text, Text, Text>  
{  
public void reduce(Text key, Iterator<Text> values,  
    OutputCollector<Text, Text> output, Reporter reporter)  
    throws IOException  
{  
	String rValue = "";
	String tValue = "";
	String sValue = "";
	String uValue = "";
	ArrayList<String> arrayListR = new ArrayList<String>();
	ArrayList<String> arrayListT = new ArrayList<String>();
	ArrayList<String> arrayListS = new ArrayList<String>();
	ArrayList<String> arrayListU = new ArrayList<String>();
	
	String stringKey = key.toString();
//	System.out.println("key: "+stringKey);
	
	
	while(values.hasNext())
	{
		String string = values.next().toString();
		//System.out.println("key" +key);
		if(string.startsWith("R,"))
		{
			rValue = string.substring(2,string.length());
			arrayListR.add(rValue);
		//	System.out.println("rValue:"+ rValue);
			//System.out.println("key" + stringKey);
		}
		else if (string.startsWith("T,"))
		{
			tValue = string.substring(2, string.length());
			arrayListT.add(tValue);
		//	System.out.println("tValue:"+ tValue);
		}
		else if(string.startsWith("S,")) {
			sValue = string.substring(2, string.length());
			arrayListS.add(sValue);
		//	System.out.println("sValue:"+ sValue);
		}
		else{
			uValue = string.substring(2,string.length());
		//	String[] strings = uValue.split(",");
			arrayListU.add(uValue);
		//	System.out.println("uValue"+ uValue);
			
		}
	}
	
	Text string =null;
	
	triangleJoin triangleJoin = new triangleJoin();
	   int num;
	   num = triangleJoin.getNum();
	   //System.out.println(num);
	   
	if(num ==1)
	{
	for(int k=0; k<arrayListT.size();k++)
	for(int l=0; l<arrayListU.size();l++)
		if(arrayListT.get(k).equals(arrayListU.get(l).split(",")[0]))
		{
			
		//	System.out.println("arrayListT:"+arrayListT.get(k)+" arrayListU:"+arrayListU.get(l));
			output.collect(key, new Text(arrayListT.get(k)+","+arrayListU.get(l).split(",")[1]+","+ key ));
			System.out.println("(RXS)XT");
		}
	
	
	for(int i=0; i< arrayListR.size(); i++)
	for(int j=0; j<arrayListS.size();j++)
	{
		output.collect(string, new Text(arrayListR.get(i) +","+ key +","+ arrayListS.get(j)));

	}
	}
	
	else if(num ==3)
	{
		for(int k=0; k<arrayListR.size();k++)
			for(int l=0; l<arrayListU.size();l++)
				if(arrayListR.get(k).equals(arrayListU.get(l).split(",")[0]))
				{
			//		System.out.println("arrayListR:"+arrayListR.get(k)+" arrayListU:"+arrayListU.get(l));
					output.collect(key, new Text( key+","+arrayListR.get(k)+","+arrayListU.get(l).split(",")[1]+"," ));
					System.out.println("(SXT)XR");
				}
		
			for(int i=0; i< arrayListT.size(); i++)
			for(int j=0; j<arrayListS.size();j++)
			{
				output.collect(string, new Text(arrayListS.get(j) +","+ key +","+ arrayListT.get(i)));

			}
			
	}
	
	
	else {
		for(int k=0; k<arrayListS.size();k++)
			for(int l=0; l<arrayListU.size();l++)
				if(arrayListS.get(k).equals(arrayListU.get(l).split(",")[1]))
				{
					
			//		System.out.println("arrayListS:"+arrayListS.get(k)+" arrayListU:"+arrayListU.get(l));
					output.collect(key, new Text(arrayListU.get(l).split(",")[0]+","+arrayListS.get(k)+","+ key));
					System.out.println("(RXT)XS");
					
				}
			
			
			
			for(int i=0; i< arrayListT.size(); i++)
			for(int j=0; j<arrayListR.size();j++)
			{
				//System.out.println("arrayListT:"+arrayListT.get(i)+" arrayListR:"+arrayListR.get(j));
				output.collect(string, new Text( key+"," +arrayListR.get(j) +","+ arrayListT.get(i)));

			}
		
	}
	
	
	
 }
 
 
}
