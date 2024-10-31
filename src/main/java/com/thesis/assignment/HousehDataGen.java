package com.thesis.assignment;
//import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import javax.xml.crypto.Data;
import java.util.Calendar;
import java.util.Random;

public class HousehDataGen extends RichSourceFunction<DataPoint> {

    private boolean running = true;
    private final Calendar cal = Calendar.getInstance();

    @Override
    public void run(SourceContext<DataPoint> sourceContext) throws Exception {
        Random rand = new Random();

        // prepare ids
        String[] houseIds = new String[10];
        for (int i = 0; i < 10; i++) {
            houseIds[i] = "houseId_" + i;
        }

        //prepare timestamp
        cal.set(2024, Calendar.JANUARY,1,0,0);
        long timestamp = cal.getTimeInMillis();

        //prepare for readings
        double[] powerRead = new double[10];

        while (running){


            for (int i = 0; i < 10; i++) {
                powerRead[i] = 10 + (rand.nextGaussian() * 20);

                sourceContext.collect(new DataPoint(houseIds[i], timestamp, powerRead[i]));
            }

            // proceed in time
            cal.add(Calendar.HOUR, 1);
            timestamp = cal.getTimeInMillis();


            Thread.sleep(10);

        }

    }

    @Override
    public void cancel() {
        this.running = false;
    }
}