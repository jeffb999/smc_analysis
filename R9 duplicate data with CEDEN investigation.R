################
#
# Purpose:  See if there are any Region 9 (San Diego) samples in CEDEN that are also in SMC production tables
#           Use SMC tables "tbl_" & "legacy_tbl_", not "unified_"
#           Use coordinates & sampling date to identify overlap (don't rely on stationcode or masterid)
#           Use original coordinates, and also coordinates rounded to 0.001 (about 100m for Mira Mar, lat or long)
#
# CEDEN data for Region 9 (San Diego) downloaded 2/8/2021
# CEDEN Chemistry analytes included alkalinity, nitrate, nitrite, nitrate + nitrite, TKN, total nitrogen, pH, suspended sediments
#
# February 2021
################




library(tidyverse)
library(sf)

#####--- GET DATA ---#####

### Import dynamically from database
require('dplyr')
require('RPostgreSQL')
library ('reshape')
# connect to db
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), host = '192.168.1.17', port = 5432, dbname = 'smc', user = 'smcread', password = '1969$Harbor')

lustations_query = "select * from sde.lu_stations"                       # Cross-walk
tbl_lustations   = tbl(con, sql(lustations_query))
lustations.1     = as.data.frame(tbl_lustations)

Chem_query = "select * from sde.tbl_chemistryresults"                    # Chemistry from tbl_
tbl_Chem   = tbl(con, sql(Chem_query))
tblChem.1     = as.data.frame(tbl_Chem, stringsAsFactors = FALSE)

LChem_query = "select * from sde.legacy_tbl_chemistryresults"            # Chemistry from legacy_tbl_
l_tbl_Chem   = tbl(con, sql(LChem_query))
leg_tblChem.1     = as.data.frame(l_tbl_Chem, stringsAsFactors = FALSE)

BMI_query = "select * from sde.tbl_taxonomyresults"                      # BMI taxonomy from tbl_
tbl_BMI   = tbl(con, sql(BMI_query))
tblBMI.1     = as.data.frame(tbl_BMI, stringsAsFactors = FALSE)

LBMI_query = "select * from sde.legacy_tbl_taxonomyresults"              # BMI taxonomy from legacy_tbl_
l_tbl_BMI   = tbl(con, sql(LBMI_query))
leg_tblBMI.1     = as.data.frame(l_tbl_BMI, stringsAsFactors = FALSE)


CEDEN.Chem <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/DataRequest/RB9 unique chem and BMI_2020/CEDEN_RB9_WaterQuality_All up to 020821.csv', stringsAsFactors=F, strip.white=TRUE)
CEDEN.BMI <- read.csv('L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/DataRequest/RB9 unique chem and BMI_2020/CEDEN_RB9_BMI_All up to 020821.csv', stringsAsFactors=F, strip.white=TRUE)


#unique(CEDEN.Chem$Analyte)


#####---  1) Standardize variable names, 2) Combine tbl_ & legacy_tbl_ data, 3) Create coordinate_date variable  ---#####


#### Chemistry

tblChem.2 <- tblChem.1 %>%
  inner_join(lustations.1[, c("stationid", "masterid", "latitude", "longitude", "probabilistic", "huc", "county", "smcshed")], # 'innerjoin' = 'all=FALSE'
             by = c("stationcode" = "stationid")) %>%
  filter(huc > 900)
#table(tblChem.2$sampledate)  # only data from 2019 & 2020

#str(tblChem.2)
tblChem.2$sampledate <- as.Date(tblChem.2$sampledate) # remove time element, and retain only date

tblChem.2$CoordDate <- paste(tblChem.2$latitude, tblChem.2$longitude, tblChem.2$sampledate, sep="_")
tblChem.2 <- tblChem.2[!duplicated(tblChem.2$CoordDate), ]  # retain only 1 coordinate/date combination (don't need records for every analyte)



leg_tblChem.2 <- leg_tblChem.1 %>%
  inner_join(lustations.1[, c("stationid", "masterid", "latitude", "longitude", "probabilistic", "huc", "county", "smcshed")], # 'innerjoin' = 'all=FALSE'
             by = c("stationcode" = "stationid")) %>%
  filter(huc > 900)
#table(leg_tblChem.2$sampledate)  # only data from 2009 to 2018
#str(leg_tblChem.2)
leg_tblChem.2$sampledate <- as.Date(leg_tblChem.2$sampledate) # remove time element, and retain only date

leg_tblChem.2$CoordDate <- paste(leg_tblChem.2$latitude, leg_tblChem.2$longitude, leg_tblChem.2$sampledate, sep="_")
leg_tblChem.2 <- leg_tblChem.2[!duplicated(leg_tblChem.2$CoordDate), ]  # retain only 1 coordinate/date combination (don't need records for every analyte)


## Combine legacy_tbl_ & tbl_
# legacy_tbl has n=40 variables & tbl_ has n=36 variables.  Therefore remove extra vars
leg_tblChem.2$legacy <- NULL
leg_tblChem.2$projectcode <- NULL
leg_tblChem.2$chemistrybatchrecordid <- NULL
leg_tblChem.2$chemistryresultsrecordid <- NULL
ltblChem.3 <- rbind(leg_tblChem.2, tblChem.2)
ltblChem.3 <- ltblChem.3[!duplicated(ltblChem.3$CoordDate), ]  # Remove dups.  Result: no dups to be removed



CEDEN.Chem.2 <- CEDEN.Chem %>%
  dplyr::rename(sampledate=SampleDate, stationcode=StationCode)
CEDEN.Chem.2$sampledate <- as.Date(as.character(CEDEN.Chem.2$sampledate), "%m/%d/%Y")

#str(CEDEN.Chem.2)
CEDEN.Chem.2 <- CEDEN.Chem.2[!is.na(CEDEN.Chem.2$TargetLatitude), ]      # remove data that don't have latitude
CEDEN.Chem.2 <- CEDEN.Chem.2[!is.na(CEDEN.Chem.2$TargetLongitude), ]     # remove data that don't have longitude
CEDEN.Chem.2$TargetLongitude <- as.numeric(CEDEN.Chem.2$TargetLongitude) # character to numeric, needed for rounding
CEDEN.Chem.2$CoordDate <- paste(CEDEN.Chem.2$TargetLatitude, CEDEN.Chem.2$TargetLongitude, CEDEN.Chem.2$sampledate, sep="_")
CEDEN.Chem.2 <- CEDEN.Chem.2[!duplicated(CEDEN.Chem.2$CoordDate), ]  # retain only 1 coordinate/date combination



#### BMI
tblBMI.2 <- tblBMI.1 %>%
  inner_join(lustations.1[, c("stationid", "masterid", "latitude", "longitude", "probabilistic", "huc", "county", "smcshed")], # 'innerjoin' = 'all=FALSE'
             by = c("stationcode" = "stationid")) %>%
  filter(huc > 900)
#table(tblBMI.2$sampledate)  # 2009 to 2020

#str(tblBMI.2)
tblBMI.2$sampledate <- as.Date(tblBMI.2$sampledate) # from POSIXct to Date

tblBMI.2$CoordDate <- paste(tblBMI.2$latitude, tblBMI.2$longitude, tblBMI.2$sampledate, sep="_")
tblBMI.2 <- tblBMI.2[!duplicated(tblBMI.2$CoordDate), ]  # retain only 1 coordinate/date combination (don't need records for every analyte)



leg_tblBMI.2 <- leg_tblBMI.1 %>%
  inner_join(lustations.1[, c("stationid", "masterid", "latitude", "longitude", "probabilistic", "huc", "county", "smcshed")], # 'innerjoin' = 'all=FALSE'
             by = c("stationcode" = "stationid")) %>%
  filter(huc > 900) # Retain Region 9
#table(leg_tblBMI.2$sampledate)  # only data from 2009 to 2018
#str(leg_tblBMI.2)
leg_tblBMI.2$sampledate <- as.Date(leg_tblBMI.2$sampledate) # from POSIXct to Date

leg_tblBMI.2$CoordDate <- paste(leg_tblBMI.2$latitude, leg_tblBMI.2$longitude, leg_tblBMI.2$sampledate, sep="_")
leg_tblBMI.2 <- leg_tblBMI.2[!duplicated(leg_tblBMI.2$CoordDate), ]  # retain only 1 coordinate/date combination (don't need records for every analyte)


## Combine legacy_tbl_ & tbl_
# legacy_tbl has n=35 variables & tbl_ has n=33 variables.  Therefore remove extra vars & conform others
leg_tblBMI.2 <- leg_tblBMI.2 %>%
  dplyr::rename(project_code=projectcode) %>%    # rename
  select(-lastchangedate,                        # remove variables
         -taxonomyresultsrecordid)

ltblBMI.3 <- rbind(leg_tblBMI.2, tblBMI.2)
ltblBMI.3 <- ltblBMI.3[!duplicated(ltblBMI.3$CoordDate), ]  # Remove dups.  Result: yes, dups removed



CEDEN.BMI.2 <- CEDEN.BMI %>%
  dplyr::rename(sampledate=SampleDate, stationcode=StationCode)
CEDEN.BMI.2$sampledate <- as.Date(as.character(CEDEN.BMI.2$sampledate), "%m/%d/%Y")
#str(CEDEN.BMI.2)
CEDEN.BMI.2 <- CEDEN.BMI.2[!is.na(CEDEN.BMI.2$TargetLatitude), ]       # remove data that don't have latitude
CEDEN.BMI.2 <- CEDEN.BMI.2[!is.na(CEDEN.BMI.2$TargetLongitude), ]      # remove data that don't have longitude
CEDEN.BMI.2 <- CEDEN.BMI.2[CEDEN.BMI.2$TargetLatitude != 'NULL', ]      # remove data that don't have longitude
CEDEN.BMI.2 <- CEDEN.BMI.2[CEDEN.BMI.2$TargetLongitude != 'NULL', ]      # remove data that don't have longitude
CEDEN.BMI.2$TargetLatitude <- as.numeric(CEDEN.BMI.2$TargetLatitude)   # character to numeric, needed for rounding
CEDEN.BMI.2$TargetLongitude <- as.numeric(CEDEN.BMI.2$TargetLongitude) # character to numeric, needed for rounding
CEDEN.BMI.2$CoordDate <- paste(CEDEN.BMI.2$TargetLatitude, CEDEN.BMI.2$TargetLongitude, CEDEN.BMI.2$sampledate, sep="_")
CEDEN.BMI.2 <- CEDEN.BMI.2[!duplicated(CEDEN.BMI.2$CoordDate), ]  # retain only 1 coordinate/date combination




#####---  Search for overlaps, using unaltered coordinates  ---#####

Overlap.Chem.1 <- CEDEN.Chem.2[CEDEN.Chem.2$CoordDate %in% ltblChem.3$CoordDate, ]   # Result: n=0

Overlap.BMI.1 <- CEDEN.BMI.2[CEDEN.BMI.2$CoordDate %in% ltblBMI.3$CoordDate, ]       # Result: n=0



#####--- Search for overlaps, round off coordinates  ---#####

# ## Test data to determine degrees associated with arbitrary 100m distance.  Use Google Earth for airbase at Mira Mar
# DistCalc <- function(w1, x1, y1, z1) {
#   Int1 <- sin(w1*pi/180)*sin(y1*pi/180) + cos(w1*pi/180)*cos(y1*pi/180)*cos(z1*pi/180-x1*pi/180)
#   Int1 <- ifelse(w1 == y1 & x1 == z1, 1, Int1)
#   Int1 <- acos(Int1) * 6371000
# }
# Distance.Meters.EW <- DistCalc(32.873184, -117.148447, 32.873184, -117.147379) # 100m difference in longitude
# DegDiffLong <- (-117.147379) - (-117.148447)                                   # 0.001068 degrees
# 
# Distance.Meters.NS <- DistCalc(32.873049, -117.147359, 32.873952, -117.147359) # 100m difference in latitude
# DegDiffLat <- 32.873952 - 32.873049                                            # 0.000903 degrees
# 
# # Therefore 100 meters ~0.001 degrees at Mira Mar (lat or long)


### Chemistry

ltblChem.3$LatRound2 <- round(ltblChem.3$latitude, digits = 3)
ltblChem.3$LongRound2 <- round(ltblChem.3$longitude, digits = 3)
ltblChem.3$CoordDate2 <- paste(ltblChem.3$LatRound2, ltblChem.3$LongRound2, ltblChem.3$sampledate, sep="_")

CEDEN.Chem.2$LatRound2 <- round(CEDEN.Chem.2$TargetLatitude, digits = 3)
CEDEN.Chem.2$LongRound2 <- round(CEDEN.Chem.2$TargetLongitude, digits = 3)
CEDEN.Chem.2$CoordDate2 <- paste(CEDEN.Chem.2$LatRound2, CEDEN.Chem.2$LongRound2, CEDEN.Chem.2$sampledate, sep="_")

Overlap.Chem.2 <- CEDEN.Chem.2[CEDEN.Chem.2$CoordDate2 %in% ltblChem.3$CoordDate2, ] # Result: n=116 overlapping samples

## Same stationcode?  
ltblChem.3$SMCstationcode <- ltblChem.3$stationcode
Overlap.Chem.2b <- Overlap.Chem.2 %>%
  inner_join(ltblChem.3[, c("CoordDate2", "SMCstationcode", "masterid", "latitude", "longitude")],
             by = c("CoordDate2" = "CoordDate2")) %>%
  dplyr::rename(SMC.lat=latitude, SMC.lon=longitude) %>%
  mutate(SameStationCode = ifelse(stationcode == SMCstationcode, "Yes", "No"))

## How far apart are non-rounded coordinates?
DistCalc <- function(w1, x1, y1, z1) {
  Int1 <- sin(w1*pi/180)*sin(y1*pi/180) + cos(w1*pi/180)*cos(y1*pi/180)*cos(z1*pi/180-x1*pi/180)
  Int1 <- ifelse(w1 == y1 & x1 == z1, 1, Int1)
  Int1 <- acos(Int1) * 6371000
}
Distance.Meters <- DistCalc(Overlap.Chem.2b$TargetLatitude, Overlap.Chem.2b$TargetLongitude, Overlap.Chem.2b$SMC.lat, Overlap.Chem.2b$SMC.lon) # 100m difference in longitude
Overlap.Chem.2b      <- data.frame(Overlap.Chem.2b, Distance.Meters)

#write.csv(Overlap.Chem.2b, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/DataRequest/RB9 unique chem and BMI_2020/Overlap samples_CEDEN v SMC_Chemistry.csv')



### BMI

ltblBMI.3$LatRound2 <- round(ltblBMI.3$latitude, digits = 3)
ltblBMI.3$LongRound2 <- round(ltblBMI.3$longitude, digits = 3)
ltblBMI.3$CoordDate2 <- paste(ltblBMI.3$LatRound2, ltblBMI.3$LongRound2, ltblBMI.3$sampledate, sep="_")

CEDEN.BMI.2$LatRound2 <- round(CEDEN.BMI.2$TargetLatitude, digits = 3)
CEDEN.BMI.2$LongRound2 <- round(CEDEN.BMI.2$TargetLongitude, digits = 3)
CEDEN.BMI.2$CoordDate2 <- paste(CEDEN.BMI.2$LatRound2, CEDEN.BMI.2$LongRound2, CEDEN.BMI.2$sampledate, sep="_")

Overlap.BMI.2 <- CEDEN.BMI.2[CEDEN.BMI.2$CoordDate2 %in% ltblBMI.3$CoordDate2, ] # Result: n=41 overlapping samples

## Same stationcode?  
ltblBMI.3$SMCstationcode <- ltblBMI.3$stationcode
Overlap.BMI.2b <- Overlap.BMI.2 %>%
  inner_join(ltblBMI.3[, c("CoordDate2", "SMCstationcode", "masterid", "latitude", "longitude")],
             by = c("CoordDate2" = "CoordDate2")) %>%
  dplyr::rename(SMC.lat=latitude, SMC.lon=longitude) %>%
  mutate(SameStationCode = ifelse(stationcode == SMCstationcode, "Yes","No"))

## How far apart using non-rounded coordinates?
DistCalc <- function(w1, x1, y1, z1) {
  Int1 <- sin(w1*pi/180)*sin(y1*pi/180) + cos(w1*pi/180)*cos(y1*pi/180)*cos(z1*pi/180-x1*pi/180)
  Int1 <- ifelse(w1 == y1 & x1 == z1, 1, Int1)
  Int1 <- acos(Int1) * 6371000
}
Distance.Meters <- DistCalc(Overlap.BMI.2b$TargetLatitude, Overlap.BMI.2b$TargetLongitude, Overlap.BMI.2b$SMC.lat, Overlap.BMI.2b$SMC.lon) # 100m difference in longitude
Overlap.BMI.2b      <- data.frame(Overlap.BMI.2b, Distance.Meters)

#write.csv(Overlap.BMI.2b, 'L:/SMC Regional Monitoring_ES/SMC_RM/Data/Working/DataRequest/RB9 unique chem and BMI_2020/Overlap samples_CEDEN v SMC_BMI.csv')






