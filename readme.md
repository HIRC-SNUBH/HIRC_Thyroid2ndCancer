Iodine131Thyroid2ndCancerRisk
==============================
<img src="https://img.shields.io/badge/Study%20Status-Repo%20Created-lightgray.svg" alt="Study Status: Repo Created">

- Analytics use case(s): Population-Level Estimation
- Study type: Clinical Application
- Tags: Thyroid2ndCancer
- Study lead: Hoyoung Lee, Sooyoung Yoo
- Study lead forums tag: **[Sooyoung Yoo](https://forums.ohdsi.org/u/sooyoung_yoo)**
- Study start date: July 15, 2020
- Study end date: 
- Protocol: **[Protocol](https://github.com/SHUBH-HIRC/HIRC_Thyroid2ndCancer/blob/master/documents/protocol_Iodine131Thyroid2ndCancerRisk.docx)**
- Publications: 
- Results explorer:

This study is for estimating the effect of Iodine-131 exposure on the occurrence of secondary cancer in survivors of thyroid cancer.

Requirements
============

- A database in [Common Data Model version 5](https://github.com/OHDSI/CommonDataModel) in one of these platforms: SQL Server, Oracle, PostgreSQL, IBM Netezza, Apache Impala, Amazon RedShift, Google BigQuery, or Microsoft APS.
- R version 3.5.0 or newer
- On Windows: [RTools](http://cran.r-project.org/bin/windows/Rtools/)
- [Java](http://java.com)
- 25 GB of free disk space

See [these instructions](https://ohdsi.github.io/MethodsLibrary/rSetup.html) on how to set up the R environment on Windows.

- Required fields of CDM and Concept_id include:
	- (Mandatory fields of CDM) : PERSON, OBSERVATION_PERIOD, CONDITION_OCCURRENCE, PROCEDURE_OCCURRENCE
	- (Mandatory Concept_id) : [Iodine 131 therapy](https://athena.ohdsi.org/search-terms/terms/4036252), [Thyroidectomy](https://athena.ohdsi.org/search-terms/terms/4030107), [Total thyroidectomy](https://athena.ohdsi.org/search-terms/terms/4073199), 

How to run
==========
1. In `R`, use the following code to install the dependencies:

	```r
	install.packages("devtools")
	library(devtools)
	install_github("ohdsi/ParallelLogger", ref = "v1.1.1")
	install_github("ohdsi/SqlRender", ref = "v1.6.3")
	install_github("ohdsi/DatabaseConnector", ref = "v2.4.1")
	install_github("ohdsi/OhdsiSharing", ref = "v0.1.3")
	install_github("ohdsi/FeatureExtraction", ref = "v2.2.5")
	install_github("ohdsi/CohortMethod", ref = "v3.1.0")
	install_github("ohdsi/EmpiricalCalibration", ref = "v2.0.0")
	install_github("ohdsi/MethodEvaluation", ref = "v1.1.0")
	```

	If you experience problems on Windows where rJava can't find Java, one solution may be to add `args = "--no-multiarch"` to each `install_github` call, for example:
	
	```r
	install_github("ohdsi/SqlRender", args = "--no-multiarch")
	```
	
	Alternatively, ensure that you have installed only the 64-bit versions of R and Java, as described in [the Book of OHDSI](https://ohdsi.github.io/TheBookOfOhdsi/OhdsiAnalyticsTools.html#installR)
	
2. In `R`, use the following `devtools` command to install the I131Thyroid2ndCancerRisk package:

	```r
	install() # Note: it is ok to delete inst/doc
	```
	
3. Once installed, you can execute the study by modifying and using the code below. For your convenience, this code is also provided under `extras/CodeToRun.R`:

	```r
	library(I131Thyroid2ndCancerRisk)
	
	# Optional: specify where the temporary files (used by the ff package) will be created:
	options(fftempdir = "c:/FFtemp")
	
	# Maximum number of cores to be used:
	maxCores <- parallel::detectCores()
	
	# Minimum cell count when exporting data:
	minCellCount <- 5
	
	# The folder where the study intermediate and result files will be written:
	outputFolder <- "c:/I131Thyroid2ndCancerRisk"
	
	# Details for connecting to the server:
	# See ?DatabaseConnector::createConnectionDetails for help
	connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
									server = "some.server.com/ohdsi",
									user = "joe",
									password = "secret")
	
	# The name of the database schema where the CDM data can be found:
	cdmDatabaseSchema <- "cdm_synpuf"
	
	# The name of the database schema and table where the study-specific cohorts will be instantiated:
	cohortDatabaseSchema <- "scratch.dbo"
	cohortTable <- "my_study_cohorts"
	
	# Some meta-information that will be used by the export function:
	databaseId <- "Synpuf"
	databaseName <- "Medicare Claims Synthetic Public Use Files (SynPUFs)"
	databaseDescription <- "Medicare Claims Synthetic Public Use Files (SynPUFs) were created to allow interested parties to gain familiarity using Medicare claims data while protecting beneficiary privacy. These files are intended to promote development of software and applications that utilize files in this format, train researchers on the use and complexities of Centers for Medicare and Medicaid Services (CMS) claims, and support safe data mining innovations. The SynPUFs were created by combining randomized information from multiple unique beneficiaries and changing variable values. This randomization and combining of beneficiary information ensures privacy of health information."
	
	# For Oracle: define a schema that can be used to emulate temp tables:
	oracleTempSchema <- NULL
	
	execute(connectionDetails = connectionDetails,
            cdmDatabaseSchema = cdmDatabaseSchema,
            cohortDatabaseSchema = cohortDatabaseSchema,
            cohortTable = cohortTable,
            oracleTempSchema = oracleTempSchema,
            outputFolder = outputFolder,
            databaseId = databaseId,
            databaseName = databaseName,
            databaseDescription = databaseDescription,
            createCohorts = TRUE,
            synthesizePositiveControls = TRUE,
            runAnalyses = TRUE,
            runDiagnostics = TRUE,
            packageResults = TRUE,
            maxCores = maxCores)
	```

4. Please send the file ```export/Results<DatabaseId>.zip``` in the output folder to the study coordinator 
(yoosoo0@gmail.com or lijbdj5051@gmail.com):

		
5. To view the results, use the Shiny app:

	```r
	prepareForEvidenceExplorer("Result<databaseId>.zip", "/shinyData")
	launchEvidenceExplorer("/shinyData", blind = TRUE)
	```
  
  Note that you can save plots from within the Shiny app. It is possible to view results from more than one database by applying `prepareForEvidenceExplorer` to the Results file from each database, and using the same data folder. Set `blind = FALSE` if you wish to be unblinded to the final results.

License
=======
The Iondine131Thyroid2ndCancerRisk package is licensed under Apache License 2.0

Development
===========
Iodine131Thyroid2ndCancerRisk was developed in ATLAS and R Studio.

### Development status

Unknown
