package pl.edu.pjwstk.s8132.add.first;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.*;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class Main {

    private static final String SMALL_FILE_PATH = "data"+File.separatorChar+"retail_small_test.dat.txt";
    private static final String EXAMPLE_FILE_PATH = "data"+File.separatorChar+"example.txt";
    private static final String FILE_PATH = "data"+File.separatorChar+"retail.dat.txt";

    private String getFilePath(String file){
        return new File(getClass().getClassLoader().getResource(file).getFile()).getAbsolutePath();
    }

    public static void main(String[] args) {
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        Main main = new Main();

        SparkConf conf = new SparkConf().setMaster("local").setAppName("ADD first app");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> data = sparkContext.textFile(main.getFilePath(EXAMPLE_FILE_PATH));
//        JavaRDD<String> data = sparkContext.textFile(main.getFilePath(SMALL_FILE_PATH));
//        JavaRDD<String> data = sparkContext.textFile(main.getFilePath(FILE_PATH));



        JavaRDD<List<String>> transactions = data.map(new Function<String, List<String>>() {
            public List<String> call(String s) throws Exception {
                String[] parts = s.split(" ");
                return Arrays.asList(parts);
            }
        });

//        FPGrowth fpGrowth = new FPGrowth().setMinSupport(0.6).setNumPartitions(10);
        FPGrowth fpGrowth = new FPGrowth().setMinSupport(0.2);
        FPGrowthModel<String> fpGrowthModel = fpGrowth.run(transactions);

        //FPGrowth
        System.out.println("tx count: " + transactions.count());
        System.out.println("FPGrowth result:");
        for(FPGrowth.FreqItemset<String> itemset: fpGrowthModel.freqItemsets().toJavaRDD().collect()){
            System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
        }

        //Association
        System.out.println("Association result:");
        double minConfidence = 0.5;
        for(AssociationRules.Rule<String> rule: fpGrowthModel.generateAssociationRules(minConfidence).toJavaRDD().collect()){
            System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }

        sparkContext.close();
    }

}