# 加载SparkR包
library(SparkR)
# 初始化 Spark context
sc <- sparkR.init(master="spark://master:7077",
                  appName='sparkr_logistic_regression',
                  sparkEnvir=list(spark.executor.memory='4g',
                                  spark.cores.max="10"))
# 从hdfs上读取txt文件，该RDD由spark集群的4个分区构成
input_rdd <- textFile(sc,"hdfs://master:7077/Rscriptmodel/data.txt",minSplits=4)
# 解析每个RDD元素的文本（在每个分区上并行）
dataset_rdd <- lapplyPartition(input_rdd, function(part) {
  part <- lapply(part, function(x) unlist(strsplit(x, '\\s')))
  part <- lapply(part, function(x) as.numeric(x[x != '']))
  part
})
# 我们需要把数据集dataset_rdd分割为训练集（train）和测试集（test）两部分，这里
# ptest为测试集的样本比例，如取ptest=0.2，即取dataset_rdd的20%样本数作为测试
# 集，80%的样本数作为训练集
split_dataset <- function(rdd, ptest) {
  #以输入样本数ptest比例创建测试集RDD
  data_test_rdd <- lapplyPartition(rdd, function(part) {
    part_test <- part[1:(length(part)*ptest)]
    part_test
  })
  # 用剩下的样本数创建训练集RDD
  data_train_rdd <- lapplyPartition(rdd, function(part) {
    part_train <- part[((length(part)*ptest)+1):length(part)]
    part_train
  })
  # 返回测试集RDD和训练集RDD的列表
  list(data_test_rdd, data_train_rdd)
}
# 接下来我们需要转化数据集为R语言的矩阵形式，并增加一列数字为1的截距项，
# 将输出项y标准化为0/1的形式
get_matrix_rdd <- function(rdd) {
  matrix_rdd <- lapplyPartition(rdd, function(part) {
    m <- matrix(data=unlist(part, F, F), ncol=25, byrow=T)
    m <- cbind(1, m)
    m[,ncol(m)] <- m[,ncol(m)]-1
    m
  })
  matrix_rdd
}
# 由于该训练集中y的值为1与0的样本数比值为7:3，所以我们需要平衡1和0的样本
# 数，使它们的样本数一致
balance_matrix_rdd <- function(matrix_rdd) {
  balanced_matrix_rdd <- lapplyPartition(matrix_rdd, function(part) {
    y <- part[,26]
    index <- sample(which(y==0),length(which(y==1)))
    index <- c(index, which(y==1))
    part <- part[index,]
    part
  })
  balanced_matrix_rdd
}
# 分割数据集为训练集和测试集
dataset <- split_dataset(dataset_rdd, 0.2)
# 创建测试集RDD
matrix_test_rdd <- get_matrix_rdd(dataset[[1]])
# 创建训练集RDD
matrix_train_rdd <- balance_matrix_rdd(get_matrix_rdd(dataset[[2]]))
# 将训练集RDD和测试集RDD放入spark分布式集群内存中
cache(matrix_test_rdd)
cache(matrix_train_rdd)
# 初始化向量theta
theta<- runif(n=25, min = -1, max = 1)
# logistic函数
hypot <- function(z) {
  1/(1+exp(-z))
}
# 损失函数的梯度计算
gCost <- function(t,X,y) {
  1/nrow(X)*(t(X)%*%(hypot(X%*%t)-y))
# 定义训练函数
train <- function(theta, rdd) {
  # 计算梯度
  gradient_rdd <- lapplyPartition(rdd, function(part) {
    X <- part[,1:25]
    y <- part[,26]
    p_gradient <- gCost(theta,X,y)
    list(list(1, p_gradient))
  })
  agg_gradient_rdd <- reduceByKey(gradient_rdd, '+', 1L)
  # 一次迭代聚合输出
  collect(agg_gradient_rdd)[[1]][[2]]
}
# 由梯度下降算法优化损失函数
# alpha ：学习速率
# steps ：迭代次数
# tol ：收敛精度
alpha <- 0.1
tol <- 1e-4
step <- 1
while(T) {
  cat("step: ",step,"\n")
  p_gradient <- train(theta, matrix_train_rdd)
  theta <- theta-alpha*p_gradient
  gradient <- train(theta, matrix_train_rdd)
  if(abs(norm(gradient,type="F")-norm(p_gradient,type="F"))<=tol) break
  step <- step+1
}
# 用训练好的模型预测测试集信贷评测结果（“good”或“bad”），并计算预测正确率
test <- lapplyPartition(matrix_test_rdd, function(part) {
    X <- part[,1:25]
    y <- part[,26]
    y_pred <- hypot(X%*%theta)
    result <- xor(as.vector(round(y_pred)),as.vector(y))
})
result<-unlist(collect(test))
corrects = length(result[result==F])
wrongs = length(result[result==T])
cat("\ncorrects: ",corrects,"\n")
cat("wrongs: ",wrongs,"\n")
cat("accuracy: ",corrects/length(y_pred),"\n")
