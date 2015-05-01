package funciones;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import entities.CeldaRendWritable;
import entities.CeldaWritable;

public class GaussianFilter implements IFilter{
	private Integer columna;
	private Integer fila;
	private double rend;
	private Integer[][] kernel = new Integer[][]{
								{1,  4,  7,  4, 1},
								{4, 16, 26, 16, 4},
								{7, 26, 41, 26, 7},
								{4, 16, 26, 16, 4},
								{1,  4,  7,  4, 1},
								};

	private final int KERNEL_SIZE = 5;
	private final int KERNEL_MIDDLE = KERNEL_SIZE / 2;
	public static final int KERNEL_VALUES_SUM = 273;


	public GaussianFilter(CeldaWritable celda, double rend) {
		this.columna = celda.getColumna().get();
		this.fila = celda.getFila().get();
		this.rend = rend;
	}

	@Override
	public List<CeldaRendWritable> getSmoothingCells() {
		ArrayList<CeldaRendWritable> celdas = new ArrayList<CeldaRendWritable>();

		CeldaWritable celda = null;
		CeldaRendWritable celdaRend = null;

		//El kernel es una matriz de 5x5. Tengo que emitir 25 celdas
		// i = fila del kernel
		// j = columna del kernel
		// Comienzo en el [2,2] (es simetrico)
		for (int i = 2; i < KERNEL_SIZE; i++)
		{
			for (int j = 2; j < KERNEL_SIZE; j++)
			{
				int posi = i - KERNEL_MIDDLE;
				int posj = j - KERNEL_MIDDLE;

				celda = new CeldaWritable(new IntWritable(this.fila+posi), new IntWritable(this.columna+posj));
				celdaRend = new CeldaRendWritable(celda, new DoubleWritable(this.rend*this.kernel[i][j]));
				celdas.add(celdaRend);
			}
		}

		return celdas;
	}



}
