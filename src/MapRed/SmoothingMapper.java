package MapRed;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import entities.CeldaWritable;

public class SmoothingMapper extends Mapper<LongWritable, Text, CeldaWritable, DoubleWritable> {

	private static final String SEPARATOR_SYMBOL = ",";
	private static final Integer MULTIPLICADOR_PPAL = 6;
	private static final Integer MULTIPLICADOR_LATERALES = 1;
	public String latitudCampo;
	public String longitudCampo;

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {

		final String linea = ivalue.toString();
		String[] datos = datosXLinea(linea);
		int col = Integer.valueOf(datos[0].split(",")[0]);
		int fila = Integer.valueOf(datos[0].split(",")[1]);
		double rend = Double.valueOf(datos[1]);
		double rendOriginal = rend * MULTIPLICADOR_PPAL;
		double rendLateral = rend * MULTIPLICADOR_LATERALES;

		// Celda original
		CeldaWritable celda = new CeldaWritable(new IntWritable(fila), new IntWritable(col));
		context.write(celda, new DoubleWritable(rendOriginal));

		// Celda fila + 1
		celda = new CeldaWritable(new IntWritable(fila + 1), new IntWritable(col));
		context.write(celda, new DoubleWritable(rendLateral));

		// Celda columna + 1
		celda = new CeldaWritable(new IntWritable(fila), new IntWritable(col + 1));
		context.write(celda, new DoubleWritable(rendLateral));

		// Celda fila -1
		celda = new CeldaWritable(new IntWritable(fila - 1), new IntWritable(col));
		context.write(celda, new DoubleWritable(rendLateral));

		// Celda columna - 1
		celda = new CeldaWritable(new IntWritable(fila), new IntWritable(col - 1));
		context.write(celda, new DoubleWritable(rendLateral));
	}

	private String[] datosXLinea(String linea) {
		linea = linea.replace("<", "").replace(">", "");
		return linea.split("	");
	}

}
