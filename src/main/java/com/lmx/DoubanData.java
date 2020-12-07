package com.lmx;

import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class DoubanData {
    public static class doubanMapper extends Mapper<LongWritable, Text, Text, Text> {
//        对电影名进行处理，去除电影名中的空格
        public String movienameData(String moviename){
            return moviename.replaceAll(" ","");
        }
//        处理评论人数方法，只保留数字
        public String commentsData(String comments) {
            comments = comments.replaceAll("[\\(\\)（）\\u4e00-\\u9fa5]", "");
            return comments;
        }
//        处理日期数据：将YYYY-m-d的数据修改成YYYY-mm-dd
        public String dateData(String dates) {
            String[] split3 = dates.split("-");
            if (split3[1].length() == 1) {
                split3[1] = "0" + split3[1];
            }
            if (split3[2].length() == 1) {
                split3[2] = "0" + split3[2];
            }
            dates = split3[0] + "-" + split3[1] + "-" + split3[2];
            return dates;
        }
//        去除演员表中的特殊符号方法
        public String acotrsData(String actors) {
            actors = actors.replaceAll("['\\[\\]\" ]", "");
            return actors;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        处理之前去除两端空格，否则分割之后数字长度可能不符合--trim（）,去除两端空格之后进行分割
            String[] split1 = value.toString().trim().toString().split("\t");
//            分割完的数据筛选数组长度为6，评分和评论人数都包含数字的数据，不合适的数据不做处理
            if (key.get() != 0 && split1.length == 6 && Pattern.compile("[0-9]").matcher(split1[1]).find()
                    && Pattern.compile("[0-9]").matcher(split1[2]).find()) {
//                将日期数据预进行处理，去除多个日期，去除汉字和字母，特殊符号等，将"/"，"年"，"月"转换成"-"，然后按照日期格式进行匹配，不符合规则的数据不做处理
                String s = split1[4].split(",")[0].split(" ")[0].split("~")[0]
                        .replaceAll("/", "-").split("\\(")[0].split("（")[0];
                if (Pattern.compile("(^\\d{4}-\\d{1,2}-\\d{1,2}$)").matcher(s).find()) {
                        context.write(new Text(split1[0]), new Text(split1[1] +
                                "\t" + commentsData(split1[2]) + "\t" + split1[3] +
                                "\t" + dateData(s) + "\t" + acotrsData(split1[5])));
                }
            }
        }
    }

    //        去除电影名重复的数据，只留下评论人数最多的一条数据
    public static String unrepeat(ArrayList<String> comments) {
        if (comments.size() > 1) {
            for (int i = 0; i < comments.size() - 1; i++) {
                for (int j = i + 1; j < comments.size(); j++) {
                    if (Integer.parseInt(comments.get(i).split("\t")[1]) < Integer.parseInt(comments.get(j).split("\t")[1])) {
                        String max = null;
                        max = comments.get(i);
                        comments.set(i, comments.get(j));
                        comments.set(j, max);
                    }
                }
            }
        }
        return comments.get(0);
    }

    public static class doubanReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> movieInfo = new ArrayList<String>();
            for (Text value : values) {
                movieInfo.add(value.toString());
            }
            context.write(key, new Text(unrepeat(movieInfo)));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, DoubanData.class.getName());
        job.setJarByClass(DoubanData.class);
        job.setMapperClass(DoubanData.doubanMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(DoubanData.doubanReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path path1 = new Path(args[0]);
        Path path2 = new Path(args[1]);
        FileInputFormat.addInputPath(job, path1);
        FileOutputFormat.setOutputPath(job, path2);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path2)) {
            fs.delete(path2, true);
        }
        if (job.waitForCompletion(true)) {
            System.out.println("--success--");
        } else {
            System.out.println("--fail--");
        }
    }
}

