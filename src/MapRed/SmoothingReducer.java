package MapRed;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import entities.CeldaWritable;
import funciones.GaussianFilter;

public class SmoothingReducer extends Reducer<CeldaWritable, DoubleWritable, CeldaWritable, DoubleWritable> {

	public void reduce(CeldaWritable _key, Iterable<DoubleWritable> values, Context context) throws IOException,
			InterruptedException {

		// process values
		double total = 0.0;
		for (DoubleWritable rend : values)
		{
			total += rend.get();
		}
		// Divido por 10 porque le doy un peso de 6 a la "celda original" y un peso
		// de 1 a las otras 4 celdas (6+4 = 10)
		double mediaRinde = total / GaussianFilter.KERNEL_VALUES_SUM;

		context.write(_key, new DoubleWritable(mediaRinde));
	}
}
