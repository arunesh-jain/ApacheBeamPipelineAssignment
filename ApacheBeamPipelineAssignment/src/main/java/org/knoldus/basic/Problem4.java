package org.knoldus.basic;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Problem4 {

    private static final String CSV_HEADER = "CustomerID,Genre,Age,Annual Income (k$)";

    public static void main(String[] args) {


        final AveragePriceProcessingOptions averagePriceProcessingOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AveragePriceProcessingOptions.class);

        Pipeline pipeline = Pipeline.create(averagePriceProcessingOptions);

        PCollection<String> data=pipeline.apply("Read-Lines", TextIO.read()
                        .from(averagePriceProcessingOptions.getInputFile()))
                .apply("Filter-Header", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)));
        PCollection<String> avgAge = data.apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(tokens[1], Integer.parseInt(tokens[2]));
                        }))
                .apply("AverageAggregation", Mean.perKey())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(productCount -> productCount.getKey() + "," + productCount.getValue()));

        PCollection<String> avgIncome = data.apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(tokens[1], Integer.parseInt(tokens[3]));
                        }))
                .apply("AverageAggregation", Mean.perKey())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(productCount -> productCount.getKey() + "," + productCount.getValue()));

        PCollectionList<String> collections = PCollectionList.of(avgAge).and(avgIncome);

        PCollection<String> merged = collections.apply(Flatten.<String>pCollections());

        merged.apply("WriteResult", TextIO.write()
                        .to(averagePriceProcessingOptions.getOutputFile())
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("Country,State,Price"));


        pipeline.run();
        System.out.println("pipeline executed successfully");
    }

    public interface AveragePriceProcessingOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("src/main/resources/source/Mall_Customers_Income.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write")
        @Default.String("src/main/resources/sink/Problem4-Output")
        String getOutputFile();
        void setOutputFile(String value);
    }
}
