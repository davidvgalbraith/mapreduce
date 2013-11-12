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
    public static HashSet<String> emitted; 
    // Maximum depth for any breadth-first search
    public static final int MAX_ITERATIONS = 10;

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
       included to demon state how to read & use the denom argument.   LIES ALLL LIES      */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {

        /* Will NOT need to modify to not lose any edges. */
        @Override
        public void map(LongWritable key, LongWritable value, Context context)
            throws IOException, InterruptedException {
            context.write(new LongWritable(key.get()), new LongWritable(value.get()));
            context.getCounter(ValueUse.EDGE).increment(1);
        }
    }

    /**Reducer of the first order. WORKS**/
    public static class LoadReduce extends Reducer<LongWritable, LongWritable, Text, TextArrayWritable> {
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

        /**Spits out the array of all things reachable from key. If the first entry is #0, that node is a starter.**/

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            double prob = 1.0 / denom;
            ArrayList<LongWritable> k = new ArrayList<LongWritable>();
            for (LongWritable e : values) {
                k.add(new LongWritable(e.get()));
            }
            if (Math.random() < prob) {
                Text[] x = new Text[k.size() + 2];
                x[0] = new Text("#" + key.get());
                x[1] = new Text("" + 1);
                for (int j = 0; j < k.size(); j += 1) {
                    x[j + 2] = new Text(k.get(j).toString());
                }
                Text[] a = new Text[1];
                a[0] = new Text("H");
                context.write(new Text("* " + key.get() + " " + key.get() + " 0"), new TextArrayWritable(a));
                context.write(new Text(key.get() + ""), new TextArrayWritable(x));
            } else {
                Text[] y = new Text[k.size()];
                for (int jj = 0; jj < k.size(); jj += 1) {
                    y[jj] = new Text(k.get(jj).toString());
                }
                context.write(new Text(key.get() + ""), new TextArrayWritable(y));
            }
        }
    }
    /**Mapper of the second order. **/
    public static class Graphmap extends Mapper<Text, TextArrayWritable, Text, Text> {

        /**This is the part that took time to write.**/

        public void map(Text key, TextArrayWritable value, Context context) throws IOException, InterruptedException {
            /**ArrayList<String> xy = new ArrayList<String>();
            for (Writable yx : value.get()) {
                xy.add(yx.toString());
            }
            System.out.println(key + " allegedly links to " + xy);**/
            if (key.toString().startsWith("*")) {
                context.write(key, new Text(""));
            } else {
                Writable[] x = value.get();
                String first = x[0].toString();
                if (first.startsWith("#")) {
                    for (int k = 2; k < x.length; k += 1) {
                        if (!first.substring(1, first.length()).equals(x[k].toString())) {
                            if (emitted.contains(first.substring(1, first.length()) + " " + x[k].toString())) {
                                System.exit(1);
                            }
                            String victory = "* " + first.substring(1, first.length()) + " " + x[k].toString() + " " + Integer.parseInt(x[1].toString());
                            emitted.add(first.substring(1, first.length()) + " " + x[k].toString());
                            context.write(new Text(victory), new Text(""));
                            String nextphase = first + " " + (1 + Integer.parseInt(x[1].toString()));
                            context.write((Text) x[k], new Text(nextphase));
                            context.write(key, (Text) x[k]);
                        }
                    }
                } else {
                    for (int kk = 0; kk < x.length; kk += 1) {
                        context.write(key, (Text) x[kk]);
                    }
                }
            }
        }
    }
    /**Reducer of the second order.**/
    public static class Graphreduce extends Reducer<Text, Text, Text, TextArrayWritable> {

        /**Sends out identity if *, actually just identity, though there's parsing. parsing.**/
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //System.out.println("Tryna reduce " + key + " right now.");
            ArrayList<Text> stuff = new ArrayList<Text>();
            for (Text t : values) {
                if (!contains(stuff, t.toString())) {
                    stuff.add(new Text(t.toString()));
                }
            }
            if (key.toString().startsWith("*")) {
                Text[] a = new Text[1];
                a[0] = new Text("I");
                context.write(key, new TextArrayWritable(a));
            } else {
                boolean boo = true;
                ArrayList<Text> hashtags = new ArrayList<Text>();
                for (Text tt : stuff) {
                    if (tt.toString().startsWith("#")) {
                        boo = false;
                        hashtags.add(new Text(tt.toString()));
                    }
                }
                for (Text ttt : hashtags) {
                    String tagged = ttt.toString();
                    Text[] y = new Text[stuff.size() + 2];
                    y[0] = new Text(tagged.substring(0, tagged.indexOf(" ")));
                    y[1] = new Text(Integer.parseInt(tagged.substring(tagged.indexOf(" ") + 1, tagged.length())) + "");
                    int offset = 0;
                    for (int kk = 2; kk < y.length; kk += 1) {
                        Text thing = stuff.get(kk - 2);
                        if (thing.toString().startsWith("#")) {
                            offset += 1;
                            continue;
                        }

                        y[kk - offset] = thing;
                    }
                    int nonull = 0;
                    for (Text lull : y) {
                        if (lull != null) {
                            nonull += 1;
                        }
                    }
                    Text[] z = new Text[nonull];
                    for (int jk = 0; jk < nonull; jk += 1) {
                        z[jk] = y[jk];
                    }
                    //System.out.println("Finished reducing " + key + " with hashtags.");
                    //      System.out.println("Hashtags were " + hashtags + "\n and stuff was " + stuff);
                    context.write(key, new TextArrayWritable(z));
                }
                if (boo) {
                    Text[] yy = new Text[stuff.size()];
                    for (int kkk = 0; kkk < yy.length; kkk += 1) {
                        yy[kkk] = stuff.get(kkk);
                    }
                    //System.out.println("Finished reducing " + key + " without hashtags.");
                    context.write(key, new TextArrayWritable(yy));
                }
            }
        }
    }

    /**Mapper of the third order.**/
    public static class Lastmap extends Mapper<Text, TextArrayWritable, LongWritable, Text> {
        
        /**Only needs to run once, disregarding key and value, since all the
           knowledge is in the HashMap already.**/
        public void map(Text key, TextArrayWritable value, Context context) throws IOException, InterruptedException {
            if (key.toString().startsWith("*")) {
                //System.out.print(key.toString() + " ");
                Scanner x = new Scanner(key.toString());
                String asterisk = x.next();
                String origin = x.next();
                String destination = x.next();
                String distance = x.next();
                context.write(new LongWritable(1), new Text(key.toString()));
            }
        }
    }


    /**Reducer of the third order.**/
    public static class Lastreduce extends Reducer<LongWritable, Text, LongWritable, LongWritable> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> x = new ArrayList<String>();
            for (Text y : values) {
                x.add(y.toString());
            }
            HashMap<Long, HashSet<String>> map = new HashMap<Long, HashSet<String>>();
            for (String z : x) {
                Scanner s = new Scanner(z);
                String astrid = s.next();
                String origin = s.next();
                String dest = s.next();
                String distance = s.next();
                if (!map.containsKey(Long.parseLong(distance))) {
                    map.put(Long.parseLong(distance), new HashSet<String>());
                }
                map.get(Long.parseLong(distance)).add(new String(origin + " " + dest));
            }
            HashMap<Long, HashSet<String>> map2 = new HashMap<Long, HashSet<String>>();
            int jjj = 0;
            for (Long plo : map.keySet()) {
                jjj += 1;
                map2.put(plo, new HashSet<String>());
            }
            for (int blob = 0; blob < jjj; blob += 1) {
                for (String str : map.get(blob)) {
                    boolean boo = true;
                    for (int a = 0; a < blob; a += 1) {
                        if (map.get(a) != null) {
                            if (map.get(a).contains(str)) {
                                boo = false;
                                break;
                            }
                        }
                    }
                    if (boo) {
                        map2.get(blob).add(new String(str));
                    }
                }
            }
            for (Long loo : map2.keySet()) {
                if (map.get(loo).equals(map2.get(loo))) {
                    System.out.println(true);
                }
                context.write(new LongWritable(loo), new LongWritable(map2.get(loo).size()));
            }
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
        emitted = new HashSet<String>();
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
        job.setOutputKeyClass(Text.class);
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
        int i = 0;
        // Will need to change terminating conditions to respond to data
        while (i<MAX_ITERATIONS) {
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
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
    /**Returns whether the ARrayLIst contians a text maching the string.**/
    static boolean contains(ArrayList<Text> x, String y) {
        for (Text a : x) {
            if (a.toString().equals(y)) {
                return true;
            }
        }
        return false;
    }
}
