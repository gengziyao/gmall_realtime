package com.atguigu.flink.day03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_source_file {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("");
        FileSource<String> source =
                FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(), new Path("F:\\Atguigu\\05_Code\\bigdata-parent\\flink\\input\\wc.txt")
                        )
                        .build();

        DataStreamSource<String> fileDS
                = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file_source");

        fileDS.print();

        env.execute();
    }
}
