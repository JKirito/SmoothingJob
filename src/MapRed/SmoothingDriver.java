package MapRed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import entities.CeldaWritable;

public class SmoothingDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");

		job.setJarByClass(SmoothingDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(SmoothingMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(SmoothingReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(CeldaWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		// TODO: specify input and output DIRECTORIES
		FileInputFormat.setInputPaths(job, new Path(
				"/home/pruebahadoop/Documentos/DataSets/monitores/CloseGaps/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("/home/pruebahadoop/Documentos/DataSets/monitores/Smoothing"));

		if (!job.waitForCompletion(true))
			return;
	}

}
