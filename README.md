# MepReduce_MutiKeywordsSort
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
        job.setJarByClass(MultiSort.class);```

  ```Java
  job.setInputFormatClass(TextInputFormat.class);```
 **此处指定类来格式化输入文件**
 ```Java
 job.setMapperClass(MyMapper.class); ```
 **指定自定义的Mapper类**
