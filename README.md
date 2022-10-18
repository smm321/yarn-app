
Compile
=======

1. mvn clean package

Run
===
```
yarn jar yarn-0.0.1-SNAPSHOT.jar com.shopee.yarn.Client 
--container_mem 1024 
--position_date ${pt_date} 
--job ${task_name} 
--env uat

```
