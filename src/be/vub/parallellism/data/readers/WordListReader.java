package be.vub.parallellism.data.readers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;

/**
 * @author Sam van den Vonder
 */

public class WordListReader {

    static public HashSet<String> read(String file) throws IOException {
        HashSet<String> resultSet = new HashSet<>();

        List<String> lines = Files.readAllLines(Paths.get(file));
        lines.forEach((String line) -> {
            if (!line.startsWith(";") && !line.isEmpty())
                resultSet.add(line);
        });
        return resultSet;
    }
}
