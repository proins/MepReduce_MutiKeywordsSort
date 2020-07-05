# MepReduce_MutiKeywordsSort
##关键代码解释
 **此处指定类来格式化输入文件:**
  ```Java
  job.setInputFormatClass(TextInputFormat.class);
  ```
 **指定自定义的Mapper类:**
 ```Java
 job.setMapperClass(MyMapper.class); 
 ```
 **指定输出<k2,v2>的类型:**
 ```Java
job.setMapOutputKeyClass(newK2.class);  
job.setMapOutputValueClass(LongWritable.class);
 ```
  **指定分区类:**
 ```Java
 job.setPartitionerClass(HashPartitioner.class);  
 job.setNumReduceTasks(1); 
 ```
   **指定自定义的reduce类:**
 ```Java
job.setReducerClass(MyReducer.class); 
 ```
   **指定输出<k3,v3>的类型:**
 ```Java
job.setOutputKeyClass(LongWritable.class);  
job.setOutputValueClass(LongWritable.class);
 ```
    **设定输出文件的格式化类:**
 ```Java
job.setOutputFormatClass(TextOutputFormat.class);
 ```
   **把代码提交给JobTracker执行:**
 ```Java
job.waitForCompletion(true); 
 ```
    **排序实现，按第一列从小到大排序；若第一列相同，按第二列从小到大排序：:**
 ```Java
public int compareTo(newK2 o) {  
     long temp = this.first -o.first;  
     if(temp!=0){  
          return (int)temp;  
          }  
        return (int)(this.second -o.second);  
     }  
 ```


##完整代码
```Java
import java.io.DataInput;  
import java.io.DataOutput;  
import java.io.IOException;  
import java.net.URI;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.io.WritableComparable;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;  
   
public class MultiSort {
    private static final String INPUT_PATH = "hdfs://h71:9000/in";  
    private static final String OUT_PATH = "hdfs://h71:9000/out";  
      
    public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();  
        FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);  
        if(fileSystem.exists(new Path(OUT_PATH))){  
            fileSystem.delete(new Path(OUT_PATH),true);  
        }  
        Job job = new Job(conf,MultiSort.class.getSimpleName());  
        FileInputFormat.setInputPaths(job, INPUT_PATH);  
        job.setJarByClass(MultiSort.class);
                job.setInputFormatClass(TextInputFormat.class);  
 
        job.setMapperClass(MyMapper.class);  

        job.setMapOutputKeyClass(newK2.class);  
        job.setMapOutputValueClass(LongWritable.class);  
          

        job.setPartitionerClass(HashPartitioner.class);  
        job.setNumReduceTasks(1);  
        
        job.setReducerClass(MyReducer.class);  

        job.setOutputKeyClass(LongWritable.class);  
        job.setOutputValueClass(LongWritable.class);  
          
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));  

        job.setOutputFormatClass(TextOutputFormat.class);  
            
        job.waitForCompletion(true);  
    }  
      
    static class MyMapper extends Mapper<LongWritable,Text, newK2,LongWritable>{  
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splied = value.toString().split(" ");
            newK2 k2 = new newK2(Long.parseLong(splied[0]),Long.parseLong(splied[1]));
            final LongWritable v2 = new LongWritable(Long.parseLong(splied[1]));
            context.write(k2, v2);
        }
    }
      
    static class MyReducer extends Reducer<newK2, LongWritable, LongWritable, LongWritable>{  
        @Override  
        protected void reduce(MultiSort.newK2 key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {  
            context.write(new LongWritable(key.first), new LongWritable(key.second));  
        }  
    }  
      
    static class newK2 implements WritableComparable<newK2>{  
        Long first;  
        Long second;  
        public newK2(long first, long second) {  
            this.first = first;  
            this.second = second;  
        }  
  
        public newK2() {  
        }  /
  
        @Override  
        public void readFields(DataInput input) throws IOException {  
            this.first = input.readLong();  
            this.second = input.readLong();  
        }  
  
        @Override  
        public void write(DataOutput out) throws IOException {  
            out.writeLong(first);  
            out.writeLong(second);  
        }   
    
           
        @Override  
        public int compareTo(newK2 o) {  
            long temp = this.first -o.first;  
            if(temp!=0){  
                return (int)temp;  
            }  
            return (int)(this.second -o.second);  
        }  
  
        @Override  
        public int hashCode() {  
            return this.first.hashCode()+this.second.hashCode();  
        }  
  
        @Override  
        public boolean equals(Object obj) {  
            if(!(obj instanceof newK2)){  
                return false;  
            }  
            newK2 k2 = (newK2)obj;  
            return(this.first == k2.first)&&(this.second == k2.second);  
        }  
    }  
}
 ```
