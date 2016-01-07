library(SparkR)
args <- commandArgs(trailing = TRUE)
if (length(args) != 1) {
  print("Usage: wordcount <file>")
  q("no")
}
# Initialize Spark context
sc <- sparkR.init(appName = "RwordCount")
lines <- textFile(sc, args[[1]])
words <- flatMap(lines,
                 function(line) {
                   strsplit(line, " ")[[1]]
                 })
wordCount <- lapply(words, function(word) { list(word, 1L) })
counts <- reduceByKey(wordCount, "+", 2L)
output <- collect(counts)
for (wordcount in output) {
  cat(wordcount[[1]], ": ", wordcount[[2]], "\n")
}
