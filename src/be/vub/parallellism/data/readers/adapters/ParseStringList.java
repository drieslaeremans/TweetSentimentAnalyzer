package be.vub.parallellism.data.readers.adapters;

import org.supercsv.cellprocessor.CellProcessorAdaptor;
import org.supercsv.util.CsvContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ParseStringList extends CellProcessorAdaptor {

    public ParseStringList() {
        super();
    }

    public Object execute(Object value, CsvContext context) {
        List<String> result = new ArrayList<>();

        if (value != null) {
            String inputValue = (String) value;
            String csv = inputValue.substring(1, inputValue.length() -1);
            String[] hashtags = csv.split(", ");
            Collections.addAll(result, hashtags);
        }
        return result;
    }
}