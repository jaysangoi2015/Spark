package com.compare;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConversions;
/**
 * This will do comparison of Json files using Spark and create a csv output of the difference.
 * The csv will have three columns
 * Key - Key whose values is different
 * type - whether the key is missing/values assiciated with key are different
 * message - difference between the values
 * 
 * Eg :- Below is the content of two josn
 * 1) Source File
 * {"id":1,"name":"ABC"}
 * {"id":2,"name":"XYZ"}
 * 
 * 2) Target files
 * {"id":1,"name":"ABC"}
 * {"id":2,"name":"PQR"}
 * {"id":3,"name":"UVW"}
 * 
 * 
 * Outpt CSV will be
 * 3 "Key Not present"                "Key is not present in source"
 * 2 "Value(s) is different"          "['name','XYZ','PQR']"    
 * 
 * @author Jay Sangoi
 *
 */
public class JsonCompareWithMap {

	public static void main(String[] args) {

		/**
		 * Input below properties
		 */
		
		String output = "" + System.currentTimeMillis(); //Give the output path
		
        String sourceFilePath = "";  // GIve the source file path
        
        String targetFilePath = "";  // GIve the target file path

		String uniqueColumn = ""; //Unique column name to be used for comparison key

		final String partitionColumn = "" ; //Partition column to be used for spark
		

		
		
		
		final SparkSession spark = SparkSession.builder().appName("SparkJsonCompare").getOrCreate();
		
		
		Dataset<Row> oldData = spark.read().json(
				sourceFilePath);

		oldData = oldData.repartition(new Column(partitionColumn));
		

		Dataset<Row> newData = spark.read().json(
				targetFilePath);

		newData = newData.repartition(new Column(partitionColumn));


		StructType structType1 = new StructType();
		structType1 = structType1.add("key1", DataTypes.StringType, false);
		structType1 = structType1.add("data1", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
				true);

		StructType structType2 = new StructType();
		structType2 = structType2.add("key2", DataTypes.StringType, false);
		structType2 = structType2.add("data2", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
				true);

		Dataset<Row> oldData1 = oldData.map(new MapFunction<Row, Row>() {

			@Override
			public Row call(Row sourceRow) throws Exception {

				return RowFactory.create(sourceRow.getString(sourceRow.fieldIndex(uniqueColumn)),
						ToScalaExample.toScalaMap(createDataMap(sourceRow)));
			}

		}, RowEncoder.apply(structType1));

		Dataset<Row> newData1 = newData.map(new MapFunction<Row, Row>() {

			@Override
			public Row call(Row sourceRow) {
				return RowFactory.create(sourceRow.getString(sourceRow.fieldIndex(uniqueColumn)),
						ToScalaExample.toScalaMap(createDataMap(sourceRow)));
			}

		}, RowEncoder.apply(structType2));

		Dataset<Row> merge = oldData1.join(newData1, oldData1.col("key1").equalTo(newData1.col("key2")), "outer");

		merge = merge.cache();


		StructType structTypeOutput = new StructType();
		structTypeOutput = structTypeOutput.add("key", DataTypes.StringType, false);
		structTypeOutput = structTypeOutput.add("type", DataTypes.StringType, false);
		structTypeOutput = structTypeOutput.add("diff", DataTypes.StringType,
				true);

		
		Dataset<Row> diff = merge.filter(new FilterFunction<Row>() {

			@Override
			public boolean call(Row value) throws Exception {

				return value.getAs("data1")== null || value.getAs("data2")== null || !value.getAs("data1").equals(value.getAs("data2")) ;
			}
		}).map(new MapFunction<Row, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Row value) throws Exception {

				String message ;
				String keyO, type;
				if(value.getAs("key1") == null) {
					//Key is not present in source
					keyO = value.getAs("key2");
					type = "Key Not present";
					message = "Key is not present in target";
				}
				else if(value.getAs("key2") == null) {
					keyO = value.getAs("key1");
					message = "Key is not present in source";
					type = "Key Not present";
				}
				else {
					type = "Value(s) is different";
					Map<String, String> val1 = JavaConversions.mapAsJavaMap(value.getAs("data1"));

					Map<String, String> val2 = JavaConversions.mapAsJavaMap(value.getAs("data2"));

					Set<String> keys = new HashSet<>();
					
					keys.addAll(val1.keySet());
					
					keys.addAll(val2.keySet());

					Diff diff = new Diff();
					diff.key = value.getString(value.fieldIndex("key1"));

					for (String key : keys) {
						if (val1.get(key) == null || val2.get(key) == null || !val1.get(key).equals(val2.get(key))) {
							diff.differentColumns.add(key);
							diff.oldValues.add(val1.get(key));
							diff.newValues.add(val2.get(key));
						}
					}
					keyO = diff.key;
					message = diff.toString();
				}

				

				return RowFactory.create(keyO, type, message);

			}
		}, RowEncoder.apply(structTypeOutput));

		diff.coalesce(1).write()
				.csv(output);

	}

	private static HashMap<String, String> createDataMap(Row sourceRow) {
		HashMap<String, String> data = new HashMap<>();
		StructType schema = sourceRow.schema();

		StructField[] fields = schema.fields();

		for (StructField field : fields) {

			String val = sourceRow.get(sourceRow.fieldIndex(field.name())).toString();

			if (field.dataType() instanceof ArrayType) {
				val = HandleArrayField.handle(val);
			}

			data.put(field.name(), val);
		}

		return data;
	}

}
