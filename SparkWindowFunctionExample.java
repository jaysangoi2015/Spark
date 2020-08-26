/**
 * @author Jay Sangoi
 *
 */
public class SparkWindowFunctionExample {

	static String output = "";

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("Preprocessor").getOrCreate();

//		spark.conf().set("spark.sql.shuffle.partitions", 400);

		spark.sparkContext().addSparkListener(new CustomSparkListener());

		Dataset<Row> text = spark.read().text("");

		text = text.withColumn("row_number", functions.monotonically_increasing_id());

		StructType schema = text.schema();

		StructField[] fields = schema.fields();
		List<StructField> fieldList = new ArrayList<>(Arrays.asList(fields));
		fieldList.add(DataTypes.createStructField("header", DataTypes.StringType, true));

		StructType st = DataTypes.createStructType(fieldList);

		Dataset<Row> map = text.map(((MapFunction<Row, Row>) sc -> {

			String value = sc.getAs("value");

			String header = null;

			if (value != null && value.length() > 1 && value.startsWith("H")) {
				int indexOf = value.indexOf(' ');

				header = value.substring(0, indexOf);

			}

			return RowFactory.create(value, sc.getAs("row_number"), header);
		}), RowEncoder.apply(st));

		map.printSchema();

		// map.show();

		// map.createOrReplaceTempView("dt");

		// Dataset<Row> sql = spark.sql("select distinct header from dt ");

		// sql.show();

		// System.out.println(sql.count());

		WindowSpec window = Window.orderBy(new Column("row_number")).rowsBetween(Window.unboundedPreceding(),
				Window.currentRow());

		map = map.withColumn("h", functions.last(new Column("header"), true).over(window));

		Dataset<Row> sql = map.repartition(new Column("h")).sortWithinPartitions(new Column("row_number"))
		// .map((MapFunction<Row, String>) sc -> sc.getAs("value"),
		// org.apache.spark.sql.Encoders.STRING())
		;

		new File(output).mkdir();

		sql.foreachPartition((ForeachPartitionFunction<Row>) SparkWindowFunctionExample::writeData);

		// System.out.println(da.count());

		// map.createOrReplaceTempView("dt");

		// Dataset<Row> sql = spark.sql("select value from dt cluster by h ");

		// sql.write().text(output);

		// d.write().partitionBy("h").sortBy("row_number")..text(output);

		// System.out.println(map.count());

//		map.foreach((ForeachFunction<Row>) sc -> System.out.println(sc));

	}

	public static void writeData(Iterator<Row> sc) throws IOException {
		Map<String, List<String>> data = new HashMap<>();
		while (sc.hasNext()) {
			Row next = sc.next();
			String key = next.getAs("h");
			data.computeIfAbsent(key, v -> new ArrayList<>());

			List<String> sb = data.get(key);
			sb.add(next.getAs("value").toString());
		}

		for (Entry<String, List<String>> d : data.entrySet()) {

			String f = output + File.separator + d.getKey() + ".W";

			Files.write(Paths.get(f), String.join("\n", d.getValue()).getBytes());

			System.out.println("Sending message to UDL for path-" + f);

		}
	}
}
