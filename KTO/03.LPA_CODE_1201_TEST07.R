##잠재계층분석변수선택231201_TEST07
library(tidyLPA)
library(dplyr)

#data import
#setwd("E:/써티웨어_2023/KTO_인구감소/01.분석/LPA")
data <- read.csv("LPA_data/LPA_data_output_1201_test07.csv")
names(data)
#data scaling & handling
names(data)


data_scaled <- data %>%
  select(names(data[c(-1)])) %>%
  single_imputation() %>%
  scale() %>% data.frame()
names(data_scaled)
data_test <- data['sgg_cd'] %>% data.frame()
data_test <- cbind(data_test, data_scaled[names(data_scaled[1:9])])
data_test['infra_avg'] <- data_scaled %>%
  select(names(data_scaled[10:12])) %>%
  rowMeans()
data_test['traffic_avg'] <- data_scaled %>%
  select(names(data_scaled[13:14])) %>%
  rowMeans()
data_test <- cbind(data_test, data_scaled[names(data_scaled[c(15)])])
names(data_test) %>% data.frame()

#LPA modeling class 개수 2~7사이로 하여 적합도 판단
DFLPA_tou <- data_test %>%
  select(names(data_test[-c(1)])) %>%
  single_imputation() %>%
  estimate_profiles(2:6)
DFLPA_tou %>% compare_solutions(statistics = c("AIC", "BIC","SABIC", "BLRT_p", "Entropy"))


get_fit(DFLPA_tou)
DFLPA_tou
DFLPA_tou %>% get_fit() %>% AHP()
# 시각화 : 그룹이 잘 나눠졌는지 확인, 적절한 class값 찾아보기
plot_profiles(DFLPA_tou$model_1_class_4)
plot_profiles(DFLPA_tou$model_1_class_5)
plot_profiles(DFLPA_tou$model_1_class_6)

# 결과 지역명 표기 저장
region_info <- read.csv("region_info.csv")
#class6개
region_class <- cbind(region_info,DFLPA_tou$model_1_class_6$dff$Class)
region_class <- region_class %>%
  rename(sgg_cd=SGG_CD,class=c(5)) %>%
  arrange(class,regn_ctgr, decreasing=TRUE)
write.csv(region_class,"TEST07_class6.csv",fileEncoding = "CP949")
region_class %>% group_by(class)%>%count()
#class6 모델링-평균/분산
est<-get_estimates(DFLPA_tou$model_1_class_6)
write.csv(est,"TEST07_estimates6.csv",fileEncoding = "CP949")

#class5개
region_class <- cbind(region_info,DFLPA_tou$model_1_class_5$dff$Class)
region_class <- region_class %>%
  rename(sgg_cd=SGG_CD,class=c(5)) %>%
  arrange(class,regn_ctgr, decreasing=TRUE)
write.csv(region_class,"TEST07_class5.csv",fileEncoding = "CP949")
region_class %>% group_by(class)%>%count()
#class5 모델링-평균/분산
est<-get_estimates(DFLPA_tou$model_1_class_5)
write.csv(est,"TEST07_estimates5.csv",fileEncoding = "CP949")

#class4개
region_class <- cbind(region_info,DFLPA_tou$model_1_class_4$dff$Class)
region_class <- region_class %>%
  rename(sgg_cd=SGG_CD,class=c(5)) %>%
  arrange(class,regn_ctgr, decreasing=TRUE)
write.csv(region_class,"TEST07_class4.csv",fileEncoding = "CP949")
region_class %>% group_by(class)%>%count()
#class4 모델링-평균/분산
est<-get_estimates(DFLPA_tou$model_1_class_4)
write.csv(est,"TEST07_estimates4.csv",fileEncoding = "CP949")

