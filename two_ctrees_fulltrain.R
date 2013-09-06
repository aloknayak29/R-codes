t <- read.table('train.tsv', sep='\t', header=TRUE)
u <- t[,c(1:2,4:27)]
miss <- u$alchemy_category == '?'
missed <- u[!miss,]
nonmiss <- u[miss,]
ind <- sample(2, nrow(missed), replace=TRUE, prob=c(0.7, 0.3))
missed$alchemy_category_score <- as.numeric(as.character(missed$alchemy_category_score))
trainData <- missed[ind==1,]
testData <- missed[ind==2,]
formula <- label ~ alchemy_category + alchemy_category_score + avglinksize + is_news + image_ratio + spelling_errors_ratio
#misfit <- ctree(formula, data=trainData)
misfit <- ctree(formula, data=missed)
#table(predict(misfit), trainData$label)
#table(predict(misfit, newdata=testData), testData$label)

ind <- sample(2, nrow(nonmiss), replace=TRUE, prob=c(0.7, 0.3))
trainData <- nonmiss[ind==1,]
testData <- nonmiss[ind==2,]
formula <- label ~ compression_ratio +  embed_ratio + framebased +frameTagRatio +  hasDomainLink +  linkwordscore +  news_front_page + avglinksize + is_news + image_ratio + spelling_errors_ratio + non_markup_alphanum_characters + numwords_in_url + parametrizedLinkRatio
#fit <- ctree(formula, data=trainData)
fit <- ctree(formula, data=nonmiss)
#table(predict(fit), trainData$label)
#table(predict(fit, newdata=testData), testData$label)

test <- read.table('test.tsv', sep='\t', header=TRUE)
test <- test[,c(1:2,4:26)]
rows <- subset(u, alchemy_category == 'unknown' | alchemy_category == 'weather' )
testm <- rbind(test, rows)

miss <- testm$alchemy_category == '?'
missedtest <- testm[!miss,]
missedtest$alchemy_category_score <- as.numeric(as.character(missedtest$alchemy_category_score))
pre <- predict(misfit, newdata=missedtest)
testm$label = 0
testm$label[which(!miss)] <- as.numeric(as.character(pre))
#subm1 <- cbind(missedtest[,2], data.frame(pre))
#names(subm) <- c('urlid', 'label')
#write.csv(subm, 'missedctreesubm.csv',row.names = F)

nonmisstest <- testm[miss,]
pre <- predict(fit, newdata=nonmisstest)
testm$label[which(miss)] <- as.numeric(as.character(pre))
#subm <- cbind(nonmisstest[,2], data.frame(pre))
#names(subm) <- c('urlid', 'label')
#write.csv(subm, 'nonmissctreesubm.csv',row.names = F)

write.csv(testm[1:3171,c(2,26)], 'diff_ctreesubm_fulltrain.csv',row.names = F)
