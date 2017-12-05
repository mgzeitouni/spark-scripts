A = LOAD 's3a://kartees-cloud-collection/spark-testing/event-inventory-small' USING PigJsonLoader();


STORE A INTO 'kartees-pig.json' USING JsonStorage();