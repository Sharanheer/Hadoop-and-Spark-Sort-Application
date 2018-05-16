import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;


	public class MyHadoopTeraSort
	{
                  
         public static void main(String[] args) throws Exception
         {
            
            Configuration conf = new Configuration();  
            //Job sortJob = new Job(conf, "Tera Sort");
			Job sortJob = Job.getInstance(conf,"Tera Sort");
            sortJob.setCombinerClass(MyHadoopReducer.class);
            sortJob.setJarByClass(MyHadoopTeraSort.class);
			
			
            //Set key
            sortJob.setOutputKeyClass(Text.class);
            //Set value 
            sortJob.setOutputValueClass(Text.class);
	
            //Set map class
            sortJob.setMapperClass(MyHadoopMapper.class);
	    	//Set reduce class
            sortJob.setReducerClass(MyHadoopReducer.class);
			
			sortJob.setPartitionerClass(MyHadoopPartitioner.class);
			sortJob.setNumReduceTasks(4);
			
            //Input path
            FileInputFormat.addInputPath(sortJob, new Path(args[0]));
            //Output path
            FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));

            System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
        }
	}
	
	class MyHadoopPartitioner extends Partitioner<Text,Text>{
		
			
		public int getPartition(Text key, Text value, int set_num_reducer){
			String data = key.toString();	
			String com = data.substring(0,1);		
			if(com.compareTo("A") < 0){
				return 0;
			}
			else if(com.compareTo("A") >= 0 && com.compareTo("Z") < 0){
				return 1;
			}
			else if(com.compareTo("Z") >= 0 && com.compareTo("m") < 0){
				return 2;
			}
			else{
				return 3;
			}
		}
	}

	class MyHadoopReducer extends Reducer<Text, Text, Text, Text>
	{
     	public void reduce(Text in_key, Text in_value, Context context_writer)
        {
                try{ 
            	Text keys = new Text();
                Text values = new Text();
                keys.set(in_key.toString() + in_value.toString());
                values.set("");
                context_writer.write(keys, values);
				}
				catch(Exception e){
					System.out.println("Some Error in reduce method");	
	        	}
		}
	}
	class MyHadoopMapper extends Mapper<Object, Text, Text, Text>
	{
        public void map(Object in_key, Text in_value, Context context_writer)
        {
		try{
        	    Text keys = new Text();
                Text values = new Text();
			        
               	String data = in_value.toString();
               	String k = data.substring(0, 10);
                String val = data.substring(10);
                keys.set(k);
                values.set(val);
                context_writer.write(keys, values);
		}
		catch(Exception e){
			System.out.println("Some Error in map method");	
	        }
                
       	}
	}
                                                             
