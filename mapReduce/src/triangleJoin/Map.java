package triangleJoin;


import java.io.IOException;

import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

 public  class Map extends MapReduceBase implements  
     Mapper<LongWritable, Text, Text, Text>  
{  
   
   
 /** 
  * Mapper接口中的map方法： 
  * void map(K1 key, V1 value, OutputCollector<K2,V2> output, Reporter reporter) 
  * 映射一个单个的输入k/v对到一个中间的k/v对 
  * 输出对不需要和输入对是相同的类型，输入对可以映射到0个或多个输出对。 
  * OutputCollector接口：收集Mapper和Reducer输出的<k,v>对。 
  * OutputCollector接口的collect(k, v)方法:增加一个(k,v)对到output 
  */  
 public void map(LongWritable key, Text value,  
         OutputCollector<Text, Text> output, Reporter reporter)  
         throws IOException  
 {  
   String path=((FileSplit)reporter.getInputSplit()).getPath().toString();
   String line = value.toString();
//   System.out.println(line);
//   System.out.println(path);
   
   Text valueText = new Text();
   Text keyText = new Text();
   
   triangleJoin triangleJoin = new triangleJoin();
   int num;
   num = triangleJoin.getNum();
 //  System.out.println("num: "+num);
   
   if(num == 1)
   {
   if(path.endsWith("R"))//IndexOf 方法  返回 String 对象内第一次出现子字符串的字符位置
   {
	  
   if(line.contains("\t"))
   {
	   String[] newLine = line.split("\t");
	   keyText.set(newLine[1]);
	   String var = "R"+ "," + newLine[0]; 
	   valueText.set(var);
	   
//	   System.out.println("keyText:"+keyText+" valueText:"+valueText);
   }
   else 
	   System.out.println("there is no tab");

   
   }
   else if(path.endsWith("S"))
   {
	   if(line.contains("\t"))
	   {
		   String[] newLine = line.split("\t");
		   keyText.set(newLine[0]);
		   String var = "S"+ "," + newLine[1]; 
		   valueText.set(var);
		   
	   }
	   else 
		   System.out.println("there is no tab");
   }
   else if(path.endsWith("T"))
   {
	   
	   if(line.contains("\t"))
	   {
		   String[] newLine = line.split("\t");
		   keyText.set(newLine[0]);
		   String var = "T"+ "," + newLine[1]; 
		   valueText.set(var);
		  
//		   System.out.println("keyText:"+keyText+" valueText:"+valueText);
	   }
	   else 
		   System.out.println("there is no tab");
	}
   else if(path.indexOf("part-00000")>0)
   {
	   String[] newLine = line.split(",");
	   keyText.set(newLine[2]);
	   String var = "U"+","+newLine[0]+","+newLine[1];
	   valueText.set(var);
	   
//	   System.out.println("kekText: "+keyText+" valueText: "+valueText);
   }
   
   else 
	   System.out.println("there is no such a file !");
   }
   
   else if (num == 3 )
   {
	   if(path.endsWith("R"))
	   {
		   

	   if(line.contains("\t"))
	   {
		   String[] newLine = line.split("\t");
		   keyText.set(newLine[0]);
		   String var = "R"+ "," + newLine[1]; 
		   valueText.set(var);
		   
//		   System.out.println("keyText:"+keyText+" valueText:"+valueText);
	   }
	   else 
		   System.out.println("there is no tab");

	   
	   }
	   else if(path.endsWith("S"))
	   {
		   if(line.contains("\t"))
		   {
			   String[] newLine = line.split("\t");
			   keyText.set(newLine[1]);
			   String var = "S"+ "," + newLine[0]; 
			   valueText.set(var);
			   
		   }
		   else 
			   System.out.println("there is no tab");
	   }
	   else if(path.endsWith("T"))
	   {
		   System.out.println(path);
		   if(line.contains("\t"))
		   {
			   String[] newLine = line.split("\t");
			   keyText.set(newLine[0]);
			   String var = "T"+ "," + newLine[1]; 
			   valueText.set(var);
			  
			  
//			   System.out.println("keyText:"+keyText+" valueText:"+valueText);
		   }
		   else 
			   System.out.println("there is no tab");
		}
	   else if(path.indexOf("part-00000")>0)
	   {
		   String[] newLine = line.split(",");
		   keyText.set(newLine[2]);
		   String var = "U"+","+newLine[0]+","+newLine[1];
		   valueText.set(var);
		   
//		   System.out.println("kekText: "+keyText+" valueText: "+valueText);
	   }
	   
	   else 
		   System.out.println("there is no such a file !");
	
   }
   
   else {
	   if(path.endsWith("R"))
	   {
		  
	   if(line.contains("\t"))
	   {
		   String[] newLine = line.split("\t");
		   keyText.set(newLine[0]);
		   String var = "R"+ "," + newLine[1]; 
		   valueText.set(var);
		   
//		   System.out.println("keyText:"+keyText+" valueText:"+valueText);
	   }
	   else 
		   System.out.println("there is no tab");

	   
	   }
	   else if(path.endsWith("S"))
	   {
		   if(line.contains("\t"))
		   {
			   String[] newLine = line.split("\t");
			   keyText.set(newLine[1]);
			   String var = "S"+ "," + newLine[0]; 
			   valueText.set(var);
			   
		   }
		   else 
			   System.out.println("there is no tab");
	   }
	   else if(path.endsWith("T"))
	   {
		   
		   if(line.contains("\t"))
		   {
			   String[] newLine = line.split("\t");
			   keyText.set(newLine[1]);
			   String var = "T"+ "," + newLine[0]; 
			   valueText.set(var);
			  
			  
//			   System.out.println("keyText:"+keyText+" valueText:"+valueText);
		   }
		   else 
			   System.out.println("there is no tab");
		}
	   else if(path.indexOf("part-00000")>0)
	   {
		   String[] newLine = line.split(",");
		   keyText.set(newLine[2]);
		   String var = "U"+","+newLine[0]+","+newLine[1];
		   valueText.set(var);
		   
//		   System.out.println("kekText: "+keyText+" valueText: "+valueText);
	   }
	   
	   else 
		   System.out.println("there is no such a file !");
		   

}
	   
   		output.collect(keyText, valueText);
   		   
   		   
 }  
}
