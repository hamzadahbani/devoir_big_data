import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VentesParVilleParAnneeJob {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private String anneeRecherchee;

        @Override
        protected void setup(Context context) {
            anneeRecherchee = context.getConfiguration().get("annee");
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] champs = value.toString().split("\\s+");
            if (champs.length == 4) {
                String date = champs[0];
                String annee = date.split("-")[0];
                if (annee.equals(anneeRecherchee)) {
                    String ville = champs[1];
                    float prix = Float.parseFloat(champs[3]);
                    context.write(new Text(ville), new FloatWritable(prix));
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float total = 0;
            for (FloatWritable val : values) {
                total += val.get();
            }
            context.write(key, new FloatWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: VentesParVilleParAnneeJob <input path> <output path> <annee>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("annee", args[2]);

        Job job = Job.getInstance(conf, "Ventes par ville pour une année donnée");
        job.setJarByClass(VentesParVilleParAnneeJob.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
