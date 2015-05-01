package entities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class CeldaRendWritable implements WritableComparable<CeldaRendWritable> {
	CeldaWritable celda;
	private DoubleWritable rend;

	public CeldaRendWritable() {
		celda = new CeldaWritable();
		rend = new DoubleWritable();
	}

	public CeldaRendWritable(CeldaWritable celda, DoubleWritable rend) {
		this.celda = celda;
		this.rend = rend;
	}

	public DoubleWritable getRend() {
		return rend;
	}

	public void setRend(DoubleWritable rend) {
		this.rend = rend;
	}

	public CeldaWritable getCelda() {
		return celda;
	}

	public void setCelda(CeldaWritable celda) {
		this.celda = celda;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		celda.readFields(in);
		rend.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		celda.write(out);
		rend.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((celda == null) ? 0 : celda.hashCode());
		result = prime * result + ((rend == null) ? 0 : rend.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CeldaRendWritable other = (CeldaRendWritable) obj;
		if (celda == null) {
			if (other.celda != null)
				return false;
		} else if (!celda.equals(other.celda))
			return false;
		if (rend == null) {
			if (other.rend != null)
				return false;
		} else if (!rend.equals(other.rend))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return celda.toString() + "	" + rend.toString();
	}

	// TODO: no estoy teniendo en cuenta el rinde...
	@Override
	public int compareTo(CeldaRendWritable cw) {
		int cmp = celda.compareTo(cw.getCelda());
		if (cmp != 0) {
			return cmp;
		}
		return rend.compareTo(cw.getRend());
	}

}
