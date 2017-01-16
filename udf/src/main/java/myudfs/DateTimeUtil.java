package myudfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class DateTimeUtil extends EvalFunc<String> {
    public String exec(Tuple input) throws IOException {
        double epoch = Double.parseDouble(input.get(0).toString());
        String format = input.get(1).toString();
        String cmd = String.format("date -d @%f +\"%%%s\"", epoch, format);
        Process p = Runtime.getRuntime().exec(cmd);
        try {
            p.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        BufferedReader reader =
                new BufferedReader(new InputStreamReader(p.getInputStream()));

        //System.out.println(reader.readLine());
        return reader.readLine();
    }
}
