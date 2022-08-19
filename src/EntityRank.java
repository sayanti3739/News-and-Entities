import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.DenseRank;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class EntityRank {

    static String arr_entity[] = {"modi","rahul","jaitley","sonia","lalu","nitish","farooq","sushma","tharoor","smriti","mamata","karunanidhi","kejriwal","sidhu","yogi","mayawati","akhilesh","chandrababu","chidambaram","fadnavis","uddhav","pawar"};

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub

        //Input dir - should contain all input json files
        String inputPath="/home/sayanti/Desktop/CS631/newsdata"; //Use absolute paths

        //Ouput dir - this directory will be created by spark. Delete this directory between each run
        String outputPath="/home/sayanti/Desktop/CS631/newsdataOutput";   //Use absolute paths

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        StructType obj = new StructType();
        obj = obj.add("entity", DataTypes.StringType, true); // false => not nullable
        obj = obj.add("source_name", DataTypes.StringType, true); // false => not nullable
        obj = obj.add("article_id", DataTypes.StringType, true); // false => not nullable
        ExpressionEncoder<Row> tupleRowEncoder = RowEncoder.apply(obj);

        SparkSession sparkSession = SparkSession.builder()
                .appName("Entity Rank")		//Name of application
                .master("local")								//Run the application on local node
                .config("spark.sql.shuffle.partitions","2")		//Number of partitions
                .getOrCreate();

        //Read multi-line JSON from input files to dataset
        Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath);

        //For initialisation of the Dataset<Row> ds
        Dataset<Row> ds = inputDataset.map(new MapFunction<Row,Row>(){
            public Row call(Row row) throws Exception {
                String entity = ((String)row.getAs("article_body"));
                entity = entity.toLowerCase();
                if(entity.contains("modi"))
                {
                    String source=((String)row.getAs("source_name"));
                    String article = ((String)row.getAs("article_id"));
                    return RowFactory.create("modi",source,article);
                }
                //If the article does not contain any of the entities
                return RowFactory.create("nokey",null,null);
            }
        }, tupleRowEncoder);
        //entity, source_name, article_id
        for(int i = 1; i<arr_entity.length; i++)
        {
            String x = arr_entity[i];
            Dataset<Row> onion = inputDataset.map(new MapFunction<Row,Row>(){
                public Row call(Row row) throws Exception {
                    String entity = ((String)row.getAs("article_body"));
                    entity = entity.toLowerCase();
                    if(entity.contains(x))
                    {
                        String source=((String)row.getAs("source_name"));
                        String article = ((String)row.getAs("article_id"));
                        return RowFactory.create(x,source,article);
                    }
                    //If the article does not contain any of the entities
                    return RowFactory.create("nokey",null,null);
                }
            }, tupleRowEncoder);
            //union of this new Dataset (containing the source and articles which contain the arr_entity[i] element) with the previous version of the dataset
            if(onion!=null)
                ds = ds.union(onion);
        }

        //Delete all rows with null values in source column
        Dataset<Row> dfs = ds.filter(ds.col("source_name"). isNotNull());
        //dfs.toJavaRDD().coalesce(1).saveAsTextFile("/home/sayanti/Desktop/CS631/newsdataOutputdfs");
        dfs.show((int)dfs.count());

        //Point (2) - count all values grouped by "entity","source"
        Dataset<Row> count = ds.groupBy("source_name","entity").count().alias("entity_source_count");
        count = count.filter(count.col("source_name") .isNotNull());
        //count.toJavaRDD().saveAsTextFile("/home/sayanti/Desktop/CS631/newsdataOutputcount");
        count.show((int)count.count());

        //Point (3)
        WindowSpec w = Window.partitionBy("source_name").orderBy(count.col("count").desc());
        Dataset<Row> leadDf = count.withColumn("rank",functions.rank().over(w));

        Dataset<Row> finaldf = leadDf.filter("rank<=5");
        finaldf = finaldf.select("source_name", "entity", "rank", "count");
        //finaldf.toJavaRDD().saveAsTextFile(outputPath);
        finaldf.show((int)finaldf.count());
    }

}
