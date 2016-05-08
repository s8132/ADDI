package pl.edu.pjwstk.s8132.add.first;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class Main {

    private static final String FILE_PATH = "data"+File.separatorChar+"retail.dat.txt";
    private static List<String> productList = new ArrayList<String>();
    private static final Double MIN_SUPPORT = 0.01;
    private static final Double MIN_CONFIDENCE = 0.8;

    private static final String SEARCH_PRODUCT = "38";
    private static List<FPGrowth.FreqItemset<String>> freqProduct = new ArrayList<FPGrowth.FreqItemset<String>>();
    private static List<AssociationRules.Rule<String>> ruleProduct = new ArrayList<AssociationRules.Rule<String>>();

    private static void disableLogger(){
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }

    public static void main(String[] args) {
        disableLogger();
        Utils utils = new Utils();

        SparkConf conf = new SparkConf().setMaster("local").setAppName("ADD first app");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        // Load data
        JavaRDD<String> data = sparkContext.textFile(utils.getFilePath(FILE_PATH));

        // Convert data
        JavaRDD<List<String>> transactions = data.map(new Function<String, List<String>>() {
            public List<String> call(String s) throws Exception {
                String[] products = s.split(" ");
                productList.addAll(Arrays.asList(products));
                return Arrays.asList(products);
            }
        });

        FPGrowth fpGrowth = new FPGrowth().setMinSupport(MIN_SUPPORT);
        FPGrowthModel<String> fpGrowthModel = fpGrowth.run(transactions);

        //FP-growth
        System.out.println("Data information:");
        System.out.println("\tTransaction size: " + transactions.count());
        System.out.println("\tProducts size: " + productList.size());
        System.out.println("\tUnique products size: " + new HashSet<String>(productList).size());
        System.out.println("\nFP-growth result (frequent sets):");

        for(FPGrowth.FreqItemset<String> itemset: fpGrowthModel.freqItemsets().toJavaRDD().collect()){
            if(itemset.javaItems().contains(SEARCH_PRODUCT)){
                freqProduct.add(itemset);
            }
            System.out.println("\t" + itemset.javaItems() + " - frequent: " + itemset.freq());
        }

        //Association
        System.out.println("\nAssociation result:");
        for(AssociationRules.Rule<String> rule: fpGrowthModel.generateAssociationRules(MIN_CONFIDENCE).toJavaRDD().collect()){
//            if(rule.javaAntecedent().contains(SEARCH_PRODUCT) || rule.javaConsequent().contains(SEARCH_PRODUCT)){
            if(rule.javaAntecedent().contains(SEARCH_PRODUCT)){
                ruleProduct.add(rule);
            }
            System.out.println("\t" + rule.javaAntecedent() + " => " + rule.javaConsequent() + ", confidence: " + String.format("%.2f", rule.confidence()));
        }

        //Result for search product
        System.out.println("\nSearch product: " + SEARCH_PRODUCT);
        System.out.println("\tFrequent sets:");
        for(FPGrowth.FreqItemset<String> itemset: freqProduct){
            System.out.println("\t\t" + itemset.javaItems() + " - frequent: " + itemset.freq());
        }
        System.out.println("\tAssociation result:");
        for(AssociationRules.Rule<String> rule: ruleProduct){
            System.out.println("\t\t" + rule.javaAntecedent() + " => " + rule.javaConsequent() + ", confidence: " + String.format("%.2f", rule.confidence()));
        }

        sparkContext.close();
    }

}