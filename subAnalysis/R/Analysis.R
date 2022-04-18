getResults <- function(outputFolder){
  library(dplyr)
  if(!dir.exists(file.path(outputFolder, "SubAnalysis"))){dir.create(file.path(outputFolder, "SubAnalysis"), recursive = TRUE)}
  specifications <- read.csv(file.path(getwd(), "subAnalysis/inst/Table1Specs.csv"), stringsAsFactors = FALSE)

  # Table1 ----------------------------------------------------------------
  resultFolder <- file.path(outputFolder, "cmOutput")
  omr <- readRDS(file.path(resultFolder, "outcomeModelReference.rds"))
  results <- omr[omr$outcomeId == 2603,]
  matchPop <- readRDS(file.path(resultFolder, results$strataFile))
  cohortMethodData <- CohortMethod::loadCohortMethodData(file.path(resultFolder, results$cohortMethodDataFile))
  covariateBalance <- CohortMethod::computeCovariateBalance(matchPop, cohortMethodData)
  CmTable1 <- CohortMethod::createCmTable1(balance = covariateBalance, specifications = specifications)
  write.csv(CmTable1, file.path(outputFolder, "SubAnalysis", "cmTable1.csv"))
  
  # calibratedValue ---------------------------------------------------------
  
  summarizeResults <- as.data.frame(CohortMethod::summarizeAnalyses(omr, resultFolder))
  hoi <- summarizeResults[summarizeResults$outcomeId == 2603,]
  negCons <- summarizeResults[summarizeResults$outcomeId != 2603, ]
  negCons <- negCons[!is.na(negCons$seLogRr), ]
  
  null <- EmpiricalCalibration::fitMcmcNull(logRr = negCons$logRr, seLogRr = negCons$seLogRr)
  analysis <- EmpiricalCalibration::calibrateP(null, logRr = hoi$logRr, seLogRr = hoi$seLogRr)
  main_results <- hoi %>% select(-c("analysisId","targetId","comparatorId","outcomeId")) %>% 
    mutate(targetIr = 1000 * eventsTarget/(targetDays/365.25),
           comparatorIr = 1000 * eventsComparator/(comparatorDays/365.25),
           cal.p = analysis$p)
  write.csv(main_results, file.path(outputFolder, "SubAnalysis", "mainResult.csv"))
  }



getSubAnalysis <- function(outputFolder, ByCumDose = FALSE){
  
  resultFolder <- file.path(outputFolder, "cmOutput")
  omr <- readRDS(file.path(resultFolder, "outcomeModelReference.rds"))
  results <- omr[omr$outcomeId == 2603,]
  matchPop <- readRDS(file.path(resultFolder, results$strataFile))
  cohortMethodData <- CohortMethod::loadCohortMethodData(file.path(resultFolder, results$cohortMethodDataFile))


  # SubAnalysis by Risk Time ------------------------------------------------
  library(dplyr)
  library(survival)
  
  ParallelLogger::logInfo("*** By Time At Risk  ***")
  
  data <- matchPop[c("subjectId", "treatment", "survivalTime", "outcomeCount", "riskStart")] %>% 
    mutate(time = as.integer(survivalTime+riskStart), Event = ifelse(outcomeCount > 0, 1, 0)) %>% 
    select(subjectId, treatment, survivalTime, time, Event)
  A = survSplit(data, cut=c(365, 1825), end="time", event="Event",start="start",id="ID2")
  A$`under 1y` <- A$treatment * (A$start < 365)
  A$`1y~5y` <- A$treatment * (A$start >= 365 & A$start < 1825)
  A$`5y~` = A$treatment * (A$start >= 1825)
  B = Surv(A$start, A$time, A$Event)
  fit = coxph(B~`under 1y`+`1y~5y`+`5y~`+cluster(ID2), data= A)
  HR = summary(fit)
  CI = confint(fit)
  
  cox.test = cox.zph(fit)
  Ta3 = matrix(nrow = 3, ncol = 5)
  colnames(Ta3) = c("HR","LL","UL","p","cox.zph_p")
  rownames(Ta3) = c("under 1y", "1y~5y","5y~")
  Ta3[1:3,1] = HR$coefficients[1:3,2]
  Ta3[1:3,2] = exp(CI[1:3,1])
  Ta3[1:3,3] = exp(CI[1:3,2])
  Ta3[1:3,4] = HR$coefficients[1:3,6]
  Ta3[2:3,5] = cox.test$table[1:2,3]
  
  write.csv(Ta3, file.path(outputFolder, "SubAnalysis", "SubResults_TAR.csv"))
  
  
  # SubAnalysis by Age ------------------------------------------------------
  
  ParallelLogger::logInfo("*** By Age Group ***")
  age <- SqlRender::readSql(file.path(getwd(), "subAnalysis/sql_subAnalysis/age.sql"))
  age.sql <- SqlRender::render(age,
                               cdm_schema = cdmDatabaseSchema,
                               cohort_schema = cohortDatabaseSchema, 
                               cohort_table = cohortTable,
                               targetid = results$targetId, 
                               comparatorid = results$comparatorId, 
                               warnOnMissingParameters = TRUE)
  age.sql <- SqlRender::translate(age.sql, targetDialect = connectionDetails$dbms)
  conn <- DatabaseConnector::connect(connectionDetails)
  age.data <- DatabaseConnector::querySql(conn, age.sql)
  disconnect(conn)
  colnames(age.data) <- SqlRender::snakeCaseToCamelCase(colnames(age.data))
  
  agePop <- matchPop %>% left_join(age.data, by = "subjectId")
  agePop_younger <- as.data.frame(agePop[agePop$age < 30,])
  agePop_older <- as.data.frame(agePop[agePop$age >= 30,])
  
  
  
  
  
  
  outcomeModel_Younger <- CohortMethod::fitOutcomeModel(population = agePop_younger,
                                              cohortMethodData = cohortMethodData,
                                              modelType = "cox",
                                              stratified = FALSE,
                                              useCovariates = TRUE,
                                              inversePtWeighting = FALSE)
  
  outcomeModel_Older <- CohortMethod::fitOutcomeModel(population = agePop_older,
                                               cohortMethodData = cohortMethodData,
                                               modelType = "cox",
                                               stratified = FALSE,
                                               useCovariates = TRUE,
                                               inversePtWeighting = FALSE)
  
  result = matrix(nrow = 2, ncol = 8)
  colnames(result) = c("t.N", "c.N", "t.O", "c.O", "HR","LL","UL", "p")
  rownames(result) = c("young", "old")
  if(outcomeModel_Younger$outcomeModelStatus == "OK"){
    young <- c(nrow(agePop_younger[agePop_younger$treatment == 1,]),
               nrow(agePop_younger[agePop_younger$treatment == 0,]),
               outcomeModel_Younger$outcomeCounts[[5]],
               outcomeModel_Younger$outcomeCounts[[6]],
               exp(outcomeModel_Younger$outcomeModelTreatmentEstimate$logRr), 
               exp(outcomeModel_Younger$outcomeModelTreatmentEstimate$logLb95),
               exp(outcomeModel_Younger$outcomeModelTreatmentEstimate$logUb95),
               2 * pmin(pnorm(coef(outcomeModel_Younger)/outcomeModel_Younger$outcomeModelTreatmentEstimate$seLogRr),
                                    1 - pnorm(coef(outcomeModel_Younger)/outcomeModel_Younger$outcomeModelTreatmentEstimate$seLogRr)))
  }else{
    young <- c(nrow(agePop_younger[agePop_younger$treatment == 1,]),
                           nrow(agePop_younger[agePop_younger$treatment == 0,]),
                           outcomeModel_Younger$outcomeCounts[[5]],
                           outcomeModel_Younger$outcomeCounts[[6]],
                           NA, 
                           NA,
                           NA,
                           NA)
  }
  if(outcomeModel_Older$outcomeModelStatus == "OK"){
    old <- c(nrow(agePop_older[agePop_older$treatment == 1,]),
                       nrow(agePop_older[agePop_older$treatment == 0,]),
                       outcomeModel_Older$outcomeCounts[[5]],
                       outcomeModel_Older$outcomeCounts[[6]],
                       exp(outcomeModel_Older$outcomeModelTreatmentEstimate$logRr), 
                       exp(outcomeModel_Older$outcomeModelTreatmentEstimate$logLb95),
                       exp(outcomeModel_Older$outcomeModelTreatmentEstimate$logUb95),
                       2 * pmin(pnorm(coef(outcomeModel_Older)/outcomeModel_Older$outcomeModelTreatmentEstimate$seLogRr),
                                1 - pnorm(coef(outcomeModel_Older)/outcomeModel_Older$outcomeModelTreatmentEstimate$seLogRr)))
  }else{
    old <- c(nrow(agePop_older[agePop_older$treatment == 1,]),
             nrow(agePop_older[agePop_older$treatment == 0,]),
             outcomeModel_Older$outcomeCounts[[5]],
             outcomeModel_Older$outcomeCounts[[6]],
             NA, 
             NA,
             NA,
             NA)
  }
  result["young", ] <- young
  result["old",] <- old
  write.csv(result, file.path(outputFolder, "SubAnalysis", "SubResults_age.csv"))
  
  # SubAnalysis by Secondary Cancer ------------------------------------------------------
  
  ParallelLogger::logInfo("*** By Cancer Type ***")
  
  sc <- SqlRender::readSql(file.path(getwd(), "subAnalysis/sql_subAnalysis/secondCx.sql"))
  sc.sql <- SqlRender::render(sc, 
                              cdm_schema = cdmDatabaseSchema,
                              cohort_schema = cohortDatabaseSchema, 
                              cohort_table = cohortTable,
                              targetid = results$targetId, 
                              comparatorid = results$comparatorId, 
                              warnOnMissingParameters = TRUE)
  
  conn <- DatabaseConnector::connect(connectionDetails)
  sc.data <- querySql(conn, sc.sql)
  DatabaseConnector::disconnect(conn)
  colnames(sc.data) <- SqlRender::snakeCaseToCamelCase(colnames(sc.data))
  sc.data <- sc.data %>% mutate(subGroup = ifelse(stringr::str_detect(conceptName, c("lymph", "leukemia", "myeloma")), "hema", "non-hema")) %>% unique()
  
  outcomeData <- matchPop %>% left_join(sc.data, by =  "subjectId") %>% as.data.frame()
  
  outcomeData_hema <- outcomeData %>% 
    mutate(survivalTime = ifelse(subGroup == "hema"& !is.na(subGroup), daysToEvent, timeAtRisk),
           outcomeCount = ifelse(subGroup == "hema"& !is.na(subGroup), 1, 0)) %>% 
    select(-colnames(sc.data)) %>% group_by(rowId) %>% slice(which.min(survivalTime))
  outcomeData_non <- outcomeData %>% 
    mutate(survivalTime = ifelse(subGroup != "hema"& !is.na(subGroup), daysToEvent, timeAtRisk),
           outcomeCount = ifelse(subGroup != "hema"& !is.na(subGroup), 1, 0)) %>% 
    select(-colnames(sc.data)) %>% group_by(rowId) %>% slice(which.min(survivalTime))
  
  outcomeModel_Hema <- CohortMethod::fitOutcomeModel(population = outcomeData_hema,
                                                     cohortMethodData = cohortMethodData,
                                                     modelType = "cox",
                                                     stratified = FALSE,                  
                                                     useCovariates = TRUE,
                                                     inversePtWeighting = FALSE)
  outcomeModel_non <- CohortMethod::fitOutcomeModel(population = outcomeData_non,
                                                    cohortMethodData = cohortMethodData,
                                                    modelType = "cox",
                                                    stratified = FALSE,
                                                    useCovariates = TRUE,
                                                    inversePtWeighting = FALSE)
  result = matrix(nrow = 2, ncol = 8)
  colnames(result) = c("t.N", "c.N", "t.O", "c.O", "HR","LL","UL", "p")
  rownames(result) = c("hema", "non-hema")
  
  if(outcomeModel_Hema$outcomeModelStatus == "OK"){
    hema <- c(nrow(outcomeData_hema[outcomeData_hema$treatment == 1,]),
              nrow(outcomeData_hema[outcomeData_hema$treatment == 0,]),
              outcomeModel_Hema$outcomeCounts[[5]],
              outcomeModel_Hema$outcomeCounts[[6]],
              exp(outcomeModel_Hema$outcomeModelTreatmentEstimate$logRr), 
              exp(outcomeModel_Hema$outcomeModelTreatmentEstimate$logLb95),
              exp(outcomeModel_Hema$outcomeModelTreatmentEstimate$logUb95),
              2 * pmin(pnorm(coef(outcomeModel_Hema)/outcomeModel_Hema$outcomeModelTreatmentEstimate$seLogRr),
                       1 - pnorm(coef(outcomeModel_Hema)/outcomeModel_Hema$outcomeModelTreatmentEstimate$seLogRr)))
    
  }else{
    hema <- c(nrow(outcomeData_hema[outcomeData_hema$treatment == 1,]),
              nrow(outcomeData_hema[outcomeData_hema$treatment == 0,]),
              outcomeModel_Hema$outcomeCounts[[5]],
              outcomeModel_Hema$outcomeCounts[[6]],
               NA, 
               NA,
               NA,
               NA)
  }
  if(outcomeModel_Hema$outcomeModelStatus == "OK"){
    Nonhema <-  c(nrow(outcomeData_non[outcomeData_non$treatment == 1,]),
                  nrow(outcomeData_non[outcomeData_non$treatment == 0,]),
                  outcomeModel_non$outcomeCounts[[5]],
                  outcomeModel_non$outcomeCounts[[6]],
                  exp(outcomeModel_non$outcomeModelTreatmentEstimate$logRr), 
                  exp(outcomeModel_non$outcomeModelTreatmentEstimate$logLb95),
                  exp(outcomeModel_non$outcomeModelTreatmentEstimate$logUb95),
                  2 * pmin(pnorm(coef(outcomeModel_non)/outcomeModel_non$outcomeModelTreatmentEstimate$seLogRr),
                           1 - pnorm(coef(outcomeModel_non)/outcomeModel_non$outcomeModelTreatmentEstimate$seLogRr)))
  }else{
    Nonhema <- c(nrow(outcomeData_non[outcomeData_non$treatment == 1,]),
                 nrow(outcomeData_non[outcomeData_non$treatment == 0,]),
                 outcomeModel_non$outcomeCounts[[5]],
                 outcomeModel_non$outcomeCounts[[6]],
                 NA, 
                 NA,
                 NA,
                 NA)
  }
  result["hema", ] <- hema
  result["non-hema", ] <-Nonhema
  write.csv(result, file.path(outputFolder, "SubAnalysis", "SubResults_cancerType.csv"))
  # results tables-----------------------------------------------------------------
  
  
  if(ByCumDose){
  # SubAnalysis by Iodine Dose ------------------------------------------------------
  
  ParallelLogger::logInfo("*** By Iodein Dose ***")
  iodineDose <- SqlRender::readSql(file.path(getwd(), "subAnalysis/sql_subAnalysis/measurement.sql"))
  iodineDose.sql <- SqlRender::render(iodineDose,
                                      cdm_schema = cdmDatabaseSchema,
                                      cohort_schema = cohortDatabaseSchema, 
                                      cohort_table = cohortTable,
                                      targetid = results$targetId, 
                                      warnOnMissingParameters = TRUE)
  iodineDose.sql <- SqlRender::translate(iodineDose.sql, targetDialect = connectionDetails$dbms)
  conn <- DatabaseConnector::connect(connectionDetails)
  iodineDose.data <- DatabaseConnector::querySql(conn, iodineDose.sql)
  disconnect(conn)
  colnames(iodineDose.data) <- SqlRender::snakeCaseToCamelCase(colnames(iodineDose.data))
  iodineDose.data_1 <- iodineDose.data %>% group_by(subjectId, cohortStartDate) %>% summarise(accum.iodine = sum(valueAsNumber)) %>% as.data.frame()
  DosePop <- matchPop[matchPop$treatment == 1,] %>% left_join(iodineDose.data_1, by = "subjectId") %>% as.data.frame()
  
  DosePop <- DosePop %>% mutate(treatment = ifelse(accum.iodine >= 37, 1, 0))
  
  outcomeModel_Dose <- CohortMethod::fitOutcomeModel(population = DosePop,
                                                     cohortMethodData = cohortMethodData,
                                                     modelType = "cox",
                                                     stratified = FALSE,
                                                     useCovariates = TRUE,
                                                     inversePtWeighting = FALSE)

  
  result = matrix(nrow = 1, ncol = 8)
  colnames(result) = c("t.N", "c.N", "t.O", "c.O", "HR","LL","UL", "p")
  rownames(result) = c("Dose")
  if(outcomeModel_Dose$outcomeModelStatus == "OK"){
  Dose <- c(nrow(DosePop[DosePop$treatment == 1,]),
                       nrow(DosePop[DosePop$treatment == 0,]),
                       outcomeModel_Dose$outcomeCounts[[5]],
                       outcomeModel_Dose$outcomeCounts[[6]],
                       exp(outcomeModel_Dose$outcomeModelTreatmentEstimate$logRr), 
                       exp(outcomeModel_Dose$outcomeModelTreatmentEstimate$logLb95),
                       exp(outcomeModel_Dose$outcomeModelTreatmentEstimate$logUb95),
                       2 * pmin(pnorm(coef(outcomeModel_Dose)/outcomeModel_Dose$outcomeModelTreatmentEstimate$seLogRr),
                                1 - pnorm(coef(outcomeModel_Dose)/outcomeModel_Dose$outcomeModelTreatmentEstimate$seLogRr)))
  
  }else{
    Dose <- c(nrow(DosePop[DosePop$treatment == 1,]),
              nrow(DosePop[DosePop$treatment == 0,]),
              outcomeModel_Dose$outcomeCounts[[5]],
              outcomeModel_Dose$outcomeCounts[[6]],
              NA, 
              NA,
              NA,
              NA)
  }
  result["Dose", ] <- Dose
  write.csv(result, file.path(outputFolder, "SubAnalysis", "SubResults_Dose.csv"))
  }
}

