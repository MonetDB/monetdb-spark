val spine = spark.range(5)
val df = spine.withColumn("b", spine("id") % 2 === 0)

df.write
	.mode("overwrite")
	.format("jdbc")
	.option("url", "jdbc:monetdb://localhost:44001/testspark?user=monetdb&password=monetdb")
	.option("dbtable", "diadia")
	//.option("driver", "org.monetdb.jdbc.MonetDriver")
	.save()

