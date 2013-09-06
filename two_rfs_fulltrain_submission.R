t <- read.table('train.tsv', sep='\t', header=TRUE)
u <- t[,c(1:2,4:27)]
library(randomForest)
miss <- u$alchemy_category == '?'
missed <- u[!miss,]
nonmiss <- u[miss,]
missed$alchemy_category_score <- as.numeric(as.character(missed$alchemy_category_score))
formula <- label ~ alchemy_category + alchemy_category_score + avglinksize + is_news + image_ratio + spelling_errors_ratio
misrf <- randomForest(formula, data=missed, ntree=100, proximity=TRUE)

formula <- label ~ compression_ratio +  embed_ratio + framebased +frameTagRatio +  hasDomainLink +  linkwordscore +  news_front_page + avglinksize + is_news + image_ratio + spelling_errors_ratio + non_markup_alphanum_characters + numwords_in_url + parametrizedLinkRatio
rf <- randomForest(formula, data=nonmiss, ntree=100, proximity=TRUE)

#test <- read.table('test.tsv', sep='\t', header=TRUE)
#test <- test[,c(1:2,4:26)]
#rows <- subset(u, alchemy_category == 'unknown' | alchemy_category == 'weather' )
#testm <- rbind(test, rows)

miss <- testm$alchemy_category == '?'
missedtest <- testm[!miss,]
missedtest$alchemy_category_score <- as.numeric(as.character(missedtest$alchemy_category_score))
pre <- predict(misrf, newdata=missedtest)
testm$label = 0
testm$label[which(!miss)] <- as.numeric(as.character(pre))

nonmisstest <- testm[miss,]
pre <- predict(rf, newdata=nonmisstest)
testm$label[which(miss)] <- as.numeric(as.character(pre))

write.csv(testm[1:3171,c(2,26)], 'diff_rfsubm_fulltrain.csv',row.names = F)
