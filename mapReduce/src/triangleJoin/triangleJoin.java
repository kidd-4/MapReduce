package triangleJoin;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapred.FileInputFormat;  
import org.apache.hadoop.mapred.FileOutputFormat;  
import org.apache.hadoop.mapred.JobClient;  
import org.apache.hadoop.mapred.JobConf;


 
/** 
 *  
 * 描述：WordCount explains by Felix 
 * @author Hadoop Dev Group 
 */  
public class triangleJoin  
{  
    /** 
     * MapReduceBase类:实现了Mapper和Reducer接口的基类（其中的方法只是实现接口，而未作任何事情） 
     * Mapper接口： 
     * WritableComparable接口：实现WritableComparable的类可以相互比较。所有被用作key的类应该实现此接口。 
     * Reporter 则可用于报告整个应用的运行进度，本例中未使用。  
     *  
     */  
	public static  int num;
	 
	
    public triangleJoin() {
		super();
		// TODO Auto-generated constructor stub
	}
    
	public static int getNum() {
		return num;
	}
	public static void setNum(int num) {
		triangleJoin.num = num;
	}
	
	public static void main(String[] args) throws Exception  
    {  
        /** 
         * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作 
         * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration conf)等 
         */  
    	long time1 = System.currentTimeMillis();
        JobConf conf = new JobConf(triangleJoin.class);  
        conf.setJobName("wordcount");           //设置一个用户定义的job名称  
        conf.setOutputKeyClass(Text.class);    //为job的输出数据设置Key类  
        conf.setOutputValueClass(Text.class);   //为job输出设置value类  
        conf.setMapperClass(Map.class);         //为job设置Mapper类  
   //     conf.setCombinerClass(Reduce.class);      //为job设置Combiner类  
        conf.setReducerClass(Reduce.class);        //为job设置Reduce类 
         //    conf.setNumMapTasks(1000);
        //      conf.setNumReduceTasks(32);
  //      conf.setInputFormat(KeyValueTextInputFormat.class);    //为map-reduce任务设置InputFormat实现类  
  //      conf.setOutputFormat(TextOutputFormat.class);  //为map-reduce任务设置OutputFormat实现类  
        /** 
         * InputFormat描述map-reduce中对job的输入定义 
         * setInputPaths():为map-reduce job设置路径数组作为输入列表 
         * setInputPath()：为map-reduce job设置路径数组作为输出列表 
         */  
          
          File fileOutput1 = new File("File/Output-1/part-00000");
          File fileOutput2 = new File("File/Output-2/part-00000");
          Path fileOut1 = new Path("File/Output-1");
          Path fileOut2 = new Path("File/Output-2");
          
          if(fileOutput1.exists() && fileOutput2.exists())
          {
        	  fileOut1.getFileSystem(conf).delete(fileOut1,true);
        	  fileOut2.getFileSystem(conf).delete(fileOut2,true);
          }
          
          String path1= new String("File/input2/R");
          String path2= new String("File/input2/S");
          String path3= new String("File/input2/T");
          
          int[] num1,num2,num3;
          int rA,rB,sB,sC,tC,tA,rSize,sSize,tSize;
          
          num1 = getNum(path1);
          rSize =num1[0]*2;
          rA = num1[1];
          rB = num1[2];
          System.out.println("rSize: " +rSize +" rA: "+rA+" rB: "+ rB);
          
          
          num2 = getNum(path2);
          sSize = num2[0]*2;
          sB = num2[1];
          sC = num2[2];
          System.out.println("sSize: " +sSize +" sB: "+sB+" sC: "+ sC);
          
          num3 = getNum(path3);
          tSize = num3[0]*2;
          tC = num3[1];
          tA = num3[2];
          System.out.println("tSize: " +tSize +" tC: "+tC+" tA: "+ tA);
        
          
          int result1,result2,result3,result4;
          result1 = (rSize*sSize)/Math.max(rB, sB);
          result2 = (rSize*tSize)/Math.max(rA, tA);
          result3 = (sSize*tSize)/Math.max(sC, tC);
          System.out.println("result1: "+ result1);
          System.out.println("result2: "+ result2);
          System.out.println("result3: "+ result3);
          result4 = getMin(result1, result2, result3);
          
          triangleJoin triangleJoin = new triangleJoin();
          if(result4==result1)
        	  triangleJoin.setNum(1); //RXS
          else if(result4 == result2)
        	  triangleJoin.setNum(2); //RXT
          else 
        	  triangleJoin.setNum(3); //SXT
          
        
          if(num == 1)
          {
          if(fileOutput1.exists())
          {
        	  FileInputFormat.addInputPath(conf, new Path("File/Output-1/part-00000"));
        	  FileInputFormat.addInputPath(conf, new Path("File/input2/T"));
        	  System.out.println("Output file exists");
        	  FileOutputFormat.setOutputPath(conf, new Path("File/Output-2"));
          }
          else
          {
        	  FileInputFormat.addInputPath(conf,new Path("File/input2/R"));
              FileInputFormat.addInputPath(conf, new Path("File/input2/S"));
              FileOutputFormat.setOutputPath(conf, new Path("File/Output-1"));
          }
          }
          else if(num==3)
          {
        	  if(fileOutput1.exists())
              {
            	  FileInputFormat.addInputPath(conf,new Path("File/input2/R"));
            	  FileInputFormat.addInputPath(conf, new Path("File/Output-1/part-00000"));
            	  System.out.println("Output file exists");
            	  FileOutputFormat.setOutputPath(conf, new Path("File/Output-2"));
              }
              else
              {
            	  FileInputFormat.addInputPath(conf, new Path("File/input2/T"));
                  FileInputFormat.addInputPath(conf, new Path("File/input2/S"));
                  FileOutputFormat.setOutputPath(conf, new Path("File/Output-1"));
              }
          }
          else {
        	  if(fileOutput1.exists())
              {
            	  FileInputFormat.addInputPath(conf,new Path("File/input2/S"));
            	  FileInputFormat.addInputPath(conf, new Path("File/Output-1/part-00000"));
            	  System.out.println("Output file exists");
            	  FileOutputFormat.setOutputPath(conf, new Path("File/Output-2"));
              }
              else
              {
                  FileInputFormat.addInputPath(conf, new Path("File/input2/R"));
                  FileInputFormat.addInputPath(conf, new Path("File/input2/T"));
                  FileOutputFormat.setOutputPath(conf, new Path("File/Output-1"));
              }
		}
          
          
//        Path fileOut = new Path("File/Output");
//        fileOut.getFileSystem(conf).delete(fileOut,true);
          
        JobClient.runJob(conf);         //运行一个job 
        long time2 = System.currentTimeMillis();
        System.out.println("Time:"+(time2-time1)+"ms");
    }  
    private static  int getMin(int num1, int num2, int num3) {
		// TODO Auto-generated method stub
		return Math.min(Math.min(num1, num2),num3);
	}
    
    private static  int[] getNum(String path) throws NumberFormatException, IOException {
    	// TODO Auto-generated method stub
    	FileInputStream fileInputStream = new FileInputStream(path);
    	InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
    	BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    	ArrayList<Integer> arrayList = new ArrayList<Integer>();
    	ArrayList<Integer> arrayList2 = new ArrayList<Integer>();
    	ArrayList<Integer> arrayList3 = new ArrayList<Integer>();
    	String string;
    	while((string= bufferedReader.readLine()) != null)
    	{
    		String[] strings = string.split("\t");
    		if(!arrayList.contains(Integer.parseInt(strings[0])))
    		arrayList.add(Integer.parseInt(strings[0]));
    		
    		if(!arrayList2.contains(Integer.parseInt(strings[1])))
    		arrayList2.add(Integer.parseInt(strings[1]));
    		
    		arrayList3.add(Integer.parseInt(strings[0]));
    		
    	}
    	int[] size = new int[3];
    	size[0] = arrayList3.size();
    	size[1] = arrayList.size();
    	size[2] = arrayList2.size();
    	return size;
    }
}
