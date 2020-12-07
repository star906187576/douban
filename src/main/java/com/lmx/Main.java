package com.lmx;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        List<String> rank=new ArrayList<String>();
        rank.add("684684");
        rank.add("456");
        rank.add("87687");
        rank.add("4568768");
        rank.add("68484");
        rank.add("78786");
        rank.add("378935");
        Collections.sort(rank);
        for (String s : rank) {
            System.out.println(s);
        }
    }
}
