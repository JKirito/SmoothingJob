package MapRed;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import entities.CeldaRendWritable;
import entities.CeldaWritable;
import funciones.GaussianFilter;
import funciones.IFilter;

public class SmoothingMapper extends Mapper<LongWritable, Text, CeldaWritable, DoubleWritable> {

	private static final String SEPARATOR_SYMBOL = ",";
	public String latitudCampo;
	public String longitudCampo;

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {

		final String linea = ivalue.toString();
		String[] datos = datosXLinea(linea);
		int col = Integer.valueOf(datos[0].split(SEPARATOR_SYMBOL)[0]);
		int fila = Integer.valueOf(datos[0].split(SEPARATOR_SYMBOL)[1]);
		double rend = Double.valueOf(datos[1]);

		CeldaWritable celda = new CeldaWritable(new IntWritable(fila), new IntWritable(col));
		IFilter smoothing = new GaussianFilter(celda, rend);
		List<CeldaRendWritable> celdasSuavizadas = smoothing.getSmoothingCells();

		for(CeldaRendWritable C : celdasSuavizadas)
		{
			context.write(C.getCelda(), C.getRend());
		}

	}

	private String[] datosXLinea(String linea) {
		linea = linea.replace("<", "").replace(">", "");
		return linea.split("	");
	}

}
