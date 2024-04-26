data = LOAD '${HIVE_TEST_TABLE}' USING org.apache.hive.hcatalog.pig.HCatLoader();
STORE data INTO '${HIVE_OUTPUT_TABLE}' USING org.apache.hive.hcatalog.pig.HCatStorer();