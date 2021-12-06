package org.knoldus.basic;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Problem1 {

    private static final String CSV_HEADER = "Price,Payment_Type,Name,City,State,Country";

    public static void main(String[] args) {


        final SumOfSales sumOfSales = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(SumOfSales.class);

        Pipeline pipeline = Pipeline.create(sumOfSales);

        pipeline.apply("Read-Lines", TextIO.read()
                        .from(sumOfSales.getInputFile()))
                .apply("Filter-Header", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)))
                .apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(tokens[7]+","+tokens[6], Double.parseDouble(tokens[2]));
                        }))
                .apply("AverageAggregation", Sum.doublesPerKey())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(productCount -> productCount.getKey() + "," + productCount.getValue()))
                .apply("WriteResult", TextIO.write()
                        .to(sumOfSales.getOutputFile())
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("Country,State,Price"));


        pipeline.run();
        System.out.println("pipeline executed successfully");
    }

    public interface SumOfSales extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("src/main/resources/source/SalesJan2009.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write")
        @Default.String("src/main/resources/sink/payment_type_count")
        String getOutputFile();
        void setOutputFile(String value);
    }
}
