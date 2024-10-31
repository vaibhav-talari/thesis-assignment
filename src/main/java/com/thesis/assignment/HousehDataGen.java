package com.thesis.assignment;
import java.util.Calendar;
import java.util.Random;

//import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class HousehDataGen extends RichSourceFunction<EMeterEvent> {

	private static final long serialVersionUID = 1L;
	private boolean running = true;
    private final Calendar cal = Calendar.getInstance();

    @Override
    public void run(SourceContext<EMeterEvent> sourceContext) throws Exception {
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

                sourceContext.collect(new EMeterEvent(houseIds[i], timestamp, powerRead[i]));
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