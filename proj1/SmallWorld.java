/*
  CS 61C Project1: Small World

  Name: David Galbraith
  Login: cs61c-ir

 */
/** Hi! Welcome to my first project. She's no speed demon,
    but she'll do the job for ya. **/

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.ArrayWritable;

public class SmallWorld {
    /**This is like 2/3 of the that goes down goes down in
       this hashmap here. HashSets have constant time contains,
       so even when I put a million things in them, they go just
       as fast. Originally, I used HashMap<Long, HashMap<Long, Integer>>
       where the outer hashmap mapped my starting vertices to hashmaps
       mapping ending vertices to path lengths, but that took  days
       to run due to very costly contains calls.
       But so I had to replace the HashMaps with HashSets in order
       to speed up the contains time, but how to capture both the path 
       length and the vertex it was to??
       Well, I'll tell you: doubles. The whole-number part of the
       double is the ending vertex and the decimal part is the path
       length divided by 100. It's beautiful, if I do say so myself.
       If your longs were near max-value, it would deliver somewhat
       unpredictable results, but like that's the only weakness, and
       max value is  big, as you may recall. For longer paths,
       I would have to change like three numbers and suddenly paths go
       up to 1000 or whathaveyou.**/
    private static HashMap<Long, HashSet<Double>> reached;
    //Yes, this is that i.
    private static int i;
    //Boo!
    private static boolean boo;
    //Booo!
    private static boolean booo;
    //justreached, an unfortunate consequence of the implementation.
    //Pretty complicated to explain here though, but suffice it to say
    //this was an utter bitch to code.
    private static HashMap<Long, HashSet<Long>> justreached;

    // Maximum depth for any breadth-first search
    public static final int MAX_ITERATIONS = 20;

    // Skeleton code uses this to share denom cmd-line arg across cluster
    public static final String DENOM_PATH = "denom.txt";

    // Example enumerated type, used by EValue and Counter example
    public static enum ValueUse {EDGE};

    // Example writable type
    public static class EValue implements Writable {
        public ValueUse use;
        public long value;

        public EValue(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public EValue() {
            this(ValueUse.EDGE, 0);
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeUTF(use.name());
            out.writeLong(value);
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            use = ValueUse.valueOf(in.readUTF());
            value = in.readLong();
        }

        public void set(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public String toString() {
            return use.name() + ": " + value;
        }
    }


    /* This example mapper loads in all edges but only propagates a subset.
       You will need to modify this to propagate all edges, but it is 
       included to demonstate how to read & use the denom argument.   LIES ALLL LIES      */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
        public long denom;

        /* Setup is called automatically once per map task. This will
           read denom in from the DistributedCache, and it will be
           available to each call of map later on via the instance
           variable.                                                  */
        @Override
        public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                Path cachedDenomPath = DistributedCache.getLocalCacheFiles(conf)[0];
                BufferedReader reader = new BufferedReader(
                                        new FileReader(cachedDenomPath.toString()));
                String denomStr = reader.readLine();
                reader.close();
                denom = Long.decode(denomStr);
            } catch (IOException ioe) {
                System.err.println("IOException reading denom from distributed cache");
                System.err.println(ioe.toString());
            }
        }

        /* Will NOT need to modify to not lose any edges. */
        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {
            // Send edge forward SPIFFILY only if part of random subset
            if (Math.random() * 50 < 1.0/denom || denom == 1) {
                if (!reached.keySet().contains(key.get())) {
                reached.put(key.get(), new HashSet<Double>());
                reached.get(key.get()).add((double) key.get());
                }
                context.write(key, value);
            } else {
                context.write(key, value);
            }
            context.getCounter(ValueUse.EDGE).increment(1);
        }
    }
    /**Reducer of the first order.**/
    public static class LoadReduce extends Reducer<LongWritable, LongWritable, LongWritable, TextArrayWritable> {

        /**Spits out the array of all things reachable from key.**/

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> x = new ArrayList<String>();
            for(LongWritable y : values) {
                x.add(y.toString());
            }
	    Object[] bananas = x.toArray();
	    Text[] pajamas = new Text[bananas.length];
	    for (int k = 0; k < bananas.length; k += 1) {
		pajamas[k] = new Text(bananas[k].toString());
	    }
	    TextArrayWritable j = new TextArrayWritable(pajamas);
            context.write(key, j);
        }
    }
    /**Mapper of the second order. **/
    public static class Graphmap extends Mapper<LongWritable, TextArrayWritable, LongWritable, Text> {

        /**This is the part that took time to write.**/

        public void map(LongWritable key, TextArrayWritable value, Context context) throws IOException, InterruptedException {
            Writable[] a = value.get();
	    for (Long yes : reached.keySet()) {
		for (int kk = 0; kk < a.length; kk += 1) {
                    Writable b = a[kk];
		    Long c = Long.parseLong(b.toString());
                    boolean boooo = true;
                    if (justreached.get(yes) != null) {
                        if (justreached.get(yes).contains(key.get())) {
                            boooo = false;
                        }
                    }
		    if (containz(reached.get(yes), key.get()) && !containz(reached.get(yes), c) && boooo) {
                        double k = (double) (i + 1) / 100 + c;
			reached.get(yes).add(k);
                        if (justreached.get(yes) == null) {
                            justreached.put(yes, new HashSet<Long>());
                        }
                        justreached.get(yes).add(c);
			boo = true;
		    }
		}	
	    }
            for (Writable q : a) {
                context.write(key, (Text) q);
            }
        }
    }
    /**Reducer of the second order.**/
    public static class Graphreduce extends Reducer<LongWritable, Text, LongWritable, TextArrayWritable> {

        /**New reduce, same as the old reduce.**/

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> x = new ArrayList<String>();
            for(Text y : values) {
                x.add(y.toString());
            }
	    Object[] bananas = x.toArray();
	    Text[] pajamas = new Text[bananas.length];
	    for (int k = 0; k < bananas.length; k += 1) {
		pajamas[k] = new Text(bananas[k].toString());
	    }
	    TextArrayWritable j = new TextArrayWritable(pajamas);
            context.write(key, j);
        }
    }

    /**Mapper of the third order.**/
    public static class Lastmap extends Mapper<LongWritable, TextArrayWritable, LongWritable, Text> {

        /**Only needs to run once, disregarding key and value, since all the
           knowledge is in the HashMap already.**/
        public void map(LongWritable key, TextArrayWritable value, Context context) throws IOException, InterruptedException {
            if (booo) {
                booo = false;
                int n = 0;
                for (Long lol : reached.keySet()) {
                    HashSet<Double> a = reached.get(lol);
                    for (Double hahaha : a) {
                        double haha = hahaha.doubleValue();
                        double laows = (haha - (int) haha) * 100 + .0001;
                        long loose = (long) laows;
                        n += 1;
                        context.write(new LongWritable(loose), new Text("This Is Bananas."));
                    }
                }
            }
        }
    }

    /**Reducer of the third order.**/
    public static class Lastreduce extends Reducer<LongWritable, Text, LongWritable, LongWritable> {
        
        /**I RUN JAVA**/
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int k = 0;
            for (Text bananas : values) {
                k += 1;
            }
            context.write(key, new LongWritable(k));
        }
    }

    // Shares denom argument across the cluster via DistributedCache
    public static void shareDenom(String denomStr, Configuration conf) {
        try {
	    Path localDenomPath = new Path(DENOM_PATH + "-source");
	    Path remoteDenomPath = new Path(DENOM_PATH);
	    BufferedWriter writer = new BufferedWriter(
				    new FileWriter(localDenomPath.toString()));
	    writer.write(denomStr);
	    writer.newLine();
	    writer.close();
	    FileSystem fs = FileSystem.get(conf);
	    fs.copyFromLocalFile(true,true,localDenomPath,remoteDenomPath);
	    DistributedCache.addCacheFile(remoteDenomPath.toUri(), conf);
        } catch (IOException ioe) {
            System.err.println("IOException writing to distributed cache");
            System.err.println(ioe.toString());
        }
    }


    public static void main(String[] rawArgs) throws Exception {
        reached = new HashMap<Long, HashSet<Double>>();
        booo = true;
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Set denom from command line arguments
        shareDenom(args[2], conf);

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(TextArrayWritable.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoadReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-" + 0 + "-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Example of reading a counter
        System.out.println("Read in " + 
                   job.getCounters().findCounter(ValueUse.EDGE).getValue() + 
                           " edges");

        // Repeats your BFS mapreduce
        i = 0;
        // Will need to change terminating conditions to respond to data
        while (i<MAX_ITERATIONS) {
            justreached = new HashMap<Long, HashSet<Long>>();
	    boo = false;
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(TextArrayWritable.class);

            job.setMapperClass(Graphmap.class);
            job.setReducerClass(Graphreduce.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i + 1) +"-out"));

            job.waitForCompletion(true);

            i++;

	    if (!boo) {
		break;
	    }
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(Lastmap.class);
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(Lastreduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
    /**Most of the stuff down here was debugged into oblivion, but the empty shells
       are preserved here as a monument to my sins.**/

    /**Finds the smallest value in the given list.**/
    int min(List<Integer> x) {
        int a = x.get(0);
        for (int b = 0; b < x.size(); b += 1) {
            if (x.get(b) < a) {
                a = x.get(b);
            }
        }
        return a;
    }
    /**Returns whether the hashmap containz the longwritable. See, that comment's
       not even true anymore, even though this is like the only function here still
       in use.**/
    static boolean containz(HashSet<Double> x, Long y) {
        for (int ii = 0; ii < MAX_ITERATIONS; ii += 1) {
            double j = (double) ii /100 + y;
            if (x.contains(j)) {
                return true;
            }
        }
        return false;
    }
    /**Returns whether the hashmap containzz the longwritable.**/
    static boolean containzz(HashMap<Long, HashMap<Long, Integer>> x, Long y) {
        for (Long z : x.keySet()) {
            if (z.equals(y)) {
                return true;
            }
        }
        return false;
    }
    public static class TextArrayWritable extends ArrayWritable {
	public TextArrayWritable() {
	    super(Text.class);
	}
	public TextArrayWritable(Text[] x) {
	    super(Text.class, x);
	}
    }
}
