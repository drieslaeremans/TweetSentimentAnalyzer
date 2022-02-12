package be.vub.parallellism.data.readers.adapters;

import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.util.CsvContext;

import java.util.List;

public class WriteStringList extends CellProcessorAdaptor {

    public WriteStringList() {
        super();
    }

    public Object execute(Object value, CsvContext context) {
        return "[" + String.join(", ", (List<String>) value) + "]";
    }
}