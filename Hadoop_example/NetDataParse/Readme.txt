分析样本工程
1.配置好MVN环境.配置本地MVN缓冲点
编译打包
mvn clean
mvn package -Dmaven.test.skip=true
mvn deploy -Dmaven.test.skip=true

执行分析程序
java -jar ./target/NetDataParse-0.1.jar -jarName=NetDataParse-0.1.jar -jobStreamName=XXXXNAME
java -jar ./target/NetDataParse-0.1.jar -jarName=NetDataParse-0.1.jar -jobStreamName=SimpleJob -dataAnalysisName=SimpleJob
java -jar ./target/NetDataParse-0.1.jar -jarName=NetDataParse-0.1.jar -jobStreamName=JstorQKParse -dataAnalysisName=JstorQKParse
或
java -jar ./target/NetDataParse-0.1.jar -jarName=NetDataParse-0.1.jar -restart -jobStreamName=XXXXNAME

编译时出现StackOverflowError增加-Xss10m，进行MAVEN编译
export MAVEN_OPTS="-Xmx512m -Xms128m -Xss10m"

FAQ
存在同样的工作流正在执行，请先退出
(这是为了防止多个程序同时执行,破坏数据设计的)

执行时增加 -dataAnalysisName=XXXXXX



