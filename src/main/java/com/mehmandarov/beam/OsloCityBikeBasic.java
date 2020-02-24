package com.mehmandarov.beam;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;


public class OsloCityBikeBasic {

    static class ExtractStationMetaDataFromJSON extends DoFn<String, KV<Integer, LinkedHashMap>> {

        private static final Logger log = LoggerFactory.getLogger(ExtractStationMetaDataFromJSON.class);

        @ProcessElement
        public void processElement(@Element String jsonElement, OutputReceiver<KV<Integer, LinkedHashMap>> receiver) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, ArrayList> map = objectMapper.readValue(jsonElement, new TypeReference<Map<String, Object>>() {});
                for (Object o : map.get("stations")) {
                    if (o != null) {
                        LinkedHashMap stationMetaDataItem = (LinkedHashMap) o;

                        // simplify the metadata object a bit
                        stationMetaDataItem.put("station_center_lat",
                                ((LinkedHashMap) stationMetaDataItem.getOrDefault("center",
                                        new LinkedHashMap<String, LinkedHashMap>())).getOrDefault("latitude", ""));
                        stationMetaDataItem.put("station_center_lon",
                                ((LinkedHashMap) stationMetaDataItem.getOrDefault("center",
                                        new LinkedHashMap<String, LinkedHashMap>())).getOrDefault("longitude", ""));
                        stationMetaDataItem.remove("center");
                        stationMetaDataItem.remove("bounds");

                        receiver.output(KV.of((Integer) stationMetaDataItem.get("id"), stationMetaDataItem));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NullPointerException e) {
                log.error("********ERROR – ExtractStationMetaDataFromJSON ******** :" + e);
            }
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * LinkedHashMap with station availability data.
     */
    public static class StationMetadata extends PTransform<PCollection<String>, PCollection<KV<Integer, LinkedHashMap>>> {
        @Override
        public PCollection<KV<Integer, LinkedHashMap>> expand(PCollection<String> elements) {

            // Convert lines of text into LinkedHashMap.
            PCollection<KV<Integer, LinkedHashMap>> stations = elements.apply(
                    ParDo.of(new ExtractStationMetaDataFromJSON()));

            return stations;
        }
    }

    /**
     * Options supported by {@link OsloCityBikeBasic}.
     *
     * <p> Defining your own configuration options. Here, you can add your own arguments
     * to be processed by the command-line parser, and specify default values for them. You can then
     * access the options values in your pipeline code.
     *
     * <p>Inherits standard configuration options.
     */
    public interface OsloCityBikeOptions extends PipelineOptions {

        /**
         * By default, the code reads from a public dataset containing a subset of
         * bike station metadata for city bikes. Set this option to choose a different input file or glob
         * (i.e. partial names with *, like "*-stations.txt").
         */
        @Description("Path of the file with the availability data")
        @Default.String("src/main/resources/bikedata-stations-example.txt")
        String getStationMetadataInputFile();
        void setStationMetadataInputFile(String value);

        /**
         * Set this required option to specify where to write the output for station availability data.
         */
        @Description("Path of the file containing station availability data")
        @Default.String("citybikes-stations-availability")
        @Validation.Required
        String getStationOutput();
        void setStationOutput(String value);

        /**
         * Set this required option to specify where to write the output for station metadata.
         */
        @Description("Path of the file containing station metadata")
        @Default.String("citybikes-stations-metadata")
        @Validation.Required
        String getMetadataOutput();
        void setMetadataOutput(String value);

    }

    static void processOsloCityBikeData(OsloCityBikeOptions options) {
        // Create a pipeline for station meta data
        Pipeline pipeline = Pipeline.create(options);

        PCollection <KV<Integer, LinkedHashMap>> stationMetadata = pipeline
                .apply("ReadLines: StationMetadataInputFiles", TextIO.read().from(options.getStationMetadataInputFile()))
                .apply(new StationMetadata());

        stationMetadata.apply(MapElements.into(TypeDescriptor.of(String.class)).via(o -> o.toString()))
                .apply("WriteStationMetaData", TextIO.write().to(options.getMetadataOutput()));
        // RETURNS:
        // KV{157, {id=157, in_service=true, title=Nylandsveien, subtitle=mellom Norbygata og Urtegata, number_of_locks=30, station_center_lat=59.91562, station_center_lon=10.762248}}
        // ---

        pipeline.run().waitUntilFinish();

    }

    public static void main(String[] args) {
        OsloCityBikeOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(OsloCityBikeOptions.class);

        processOsloCityBikeData(options);
    }
}


/*
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`


mvn compile exec:java \
      -Pdirect-runner \
      -Dexec.mainClass=com.mehmandarov.beam.OsloCityBikeBasic \
      -Dexec.args=" \
        --stationMetadataInputFile=src/main/resources/bikedata-stations-example.txt \
        --stationOutput=bikedatalocal"
 */
