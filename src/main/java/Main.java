import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;


public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> diskfile = javaSparkContext.textFile("employers.txt");
        diskfile
                .flatMap(x -> Arrays.asList(x.split("\n"))
                        .iterator())
                .map(x -> {
                    String[] splitNameAgeAndWork = x.split(",");
                    return new Person()
                            .setName(splitNameAgeAndWork[0].split("=")[1])
                            .setAge(Integer.valueOf(splitNameAgeAndWork[1].split("=")[1]))
                            .setWork(splitNameAgeAndWork[2].split("=")[1]);
                })
                .foreach(x -> System.out.println(x));
    }
}
