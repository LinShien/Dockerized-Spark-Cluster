 #!/bin/bash

#/spark/bin/spark-submit \
#--class ${SPARK_APPLICATION_MAIN_CLASS} \
#--master ${SPARK_MASTER_URL} \
#--deploy-mode cluster \
#--total-executor-cores 1 \
# ${SPARK_SUBMIT_ARGS} \
# ${SPARK_APPLICATION_JAR_LOCATION} \
# ${SPARK_APPLICATION_ARGS} \

/spark/bin/spark-submit  \
--master ${SPARK_MASTER_URL} \
--total-executor-cores 2 \
${SPARK_PY_APPLICATION}
