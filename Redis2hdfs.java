import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import redis.clients.jedis.Jedis;

public class Redis2hdfs { 

    public static void main(String[] args) throws java.io.IOException { 

        if (args.length<1) { 
            System.err.println("Need an output path!"); 
            System.exit(1);
        }
        String outputPath=args[0];

        int redisPort=6379;
        String redisHost="localhost";
        String redisUrl = System.getenv("REDIS_URL");
        if (redisUrl!=null) { 
            redisHost=redisUrl.replaceAll(":.*$","");
            redisPort=Integer.parseInt( redisUrl.replaceAll("^.*:",""));
        }
        Jedis jedis=new Jedis(redisHost,redisPort) ; // connect to Redis

        // HDFS setup
        Configuration conf = new Configuration();
        conf.addResource(new Path("/opt/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/opt/hadoop/etc/hadoop/hdfs-site.xml"));
        FileSystem filesystem = FileSystem.get(conf);

        // keep reading from Redis until the flag is lowered
        do {
            long qlen=jedis.llen("TR_QUEUE");
            if (qlen>0) { 
                pop(jedis,filesystem,outputPath); 
            } else { 
                System.out.print(".");
                sleep(5); 
            } 
        }
        while( jedis.exists("TR_FLAG") );  
        jedis.close();
    }

    public static void sleep(int time) { 
        try {
            Thread.sleep( 1000* time) ; 
        } catch (InterruptedException ie) {
            System.err.println("An InterruptedException was thrown in my sleep!");
        }
    }

    // Pop a key from the queue, fetch the corresponding 
    // value from the buffer and write it to HDFS.
    public static void pop(Jedis jedis, 
                           FileSystem filesystem, 
                           String outputPath
                           ) throws java.io.IOException { 

        // pop 1 value from the queue
        String key=jedis.rpop("TR_QUEUE"); 
        if (key.length()<1) {
            System.err.println("Received empty key");
            return;
        }
        
        // read the corresponding buffer 
        String value=jedis.hget("TR_BUFFER",key);
        if ( (value==null) || (value.length()<1) ) {
            System.err.println("Received empty value");
            return;
        }

        // write value to file on HDFS
        Path outFile = new Path(outputPath+"/"+key);
        if (filesystem.exists(outFile)) 
            throw new IOException("Output already exists");
        try( 
            FSDataOutputStream out = filesystem.create(outFile); 
            BufferedWriter bw = new BufferedWriter(
                                    new OutputStreamWriter(out));
        ) { 
            bw.write(value);
        }
        System.out.println("Wrote: " + outFile);

        // the key/value can now be removed from the buffer 
        long jv=jedis.hdel("TR_BUFFER",key);
        if (jv!=1) {
            System.err.println("HDEL returned "+jv);
            return;
        }
    } //end_pop
}
