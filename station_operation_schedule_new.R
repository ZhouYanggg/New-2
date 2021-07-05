################################################################################
# HYDAT INCONSISTENCY ADJUSTMENT PROJECT
# The following script deal with the issue of inconsistencies within hydat.
# We focus primarily on three tables:
# stn_op_sched(hy_stn_op_scheduel()) in the format of:
# STATION_NUMBER  DATA_TYPE  Year  Month_from  Month_to 
# stn_data_col(hy_stn_data_coll());
# STATION_NUMBER  DATA_TYPE  Year_from  Year_to  MEASUREMENT OPERATION
# stn_data_ran(hy_stn_data_range()):
# STATION_NUMBER  DATA_TYPE  SED_DATA_TYPE  Year_from  Year_to  RECORD_LENGTH
# However, there are numerous inconsistencies in between those tables and 
# the actual data recorded within HYDAT established using functions
# hy_monthly_flows() and hy_monthly_levels().
# The script consists of two sections:
# 1. Establishing new versions for each of the three tables based on hydat
#    using the identical format with the original table.
# 2. Outputting three tables that keeps track of the inconsistencies within
#    each of the three sets of tables
################################################################################

####### Libraries ##############################################################
library(tidyhydat)
library(dplyr)
library(tidyr)
#library(pbapply)
#library(purrr)

# Connecting to three tables from tidyhydat#####################################
stn_op_sched <- hy_stn_op_schedule()
stn_data_col <- hy_stn_data_coll()
stn_data_ran <- hy_stn_data_range()
################################################################################

# Establishing list unique stations from each of the three tables
sched_list = unique((stn_op_sched %>% filter(DATA_TYPE != "Sediment in mg/L"))[["STATION_NUMBER"]])
col_list = unique((stn_data_col %>% filter(DATA_TYPE == "Flow"| DATA_TYPE == "Water Level"))[["STATION_NUMBER"]])
ran_list = unique((stn_data_ran %>% filter(DATA_TYPE == "Q" |DATA_TYPE == "H"))[["STATION_NUMBER"]])
################################################################################

col_not_ran = col_list[!(col_list %in% ran_list)]
col_not_sched = col_list[!(col_list %in% sched_list)]
#sched_not_ran = sched_list[!(sched_list %in% ran_list)]
#sched_not_col = sched_list[!(sched_list %in% col_list)]
#ran_not_col = ran_list[!(ran_list %in% col_list)]
ran_not_sched = ran_list[!(ran_list %in% sched_list)]
################################################################################



################# SECTION 1 BUILDING TABLES BASED ON HYDAT #####################
# create empty data frames
stn_op_sched_new = data.frame()
stn_data_col_new = data.frame()
stn_data_ran_new = data.frame()

########## stn_op_sched ########################################################
# When creating the new table, we record the following data:
# STATION_NUMBER  DATA_TYPE  Year  Month_from  Month_to 
# Note that a huge portion of the Month_from and Month_to columns were empty
# in the original table, this new table will fill in all the gaps when ever applicable.
# checking hydat for data for the same sets of stations as in stn_op_sched
# takes ~35 min to run
for (i in sched_list){
  # if hydat contains flow data and water level data for the given station
  flow_demo = try(hy_monthly_flows(i), TRUE)
  level_demo = try(hy_monthly_levels(i), TRUE)
  # two parallel if's to add onto the empty data frame
  if (!(class(flow_demo)[1]=="try-error")){
    summ = hy_monthly_flows(i) %>% group_by(Year) %>% 
    summarise(min = month.abb[min(Month)], max = month.abb[max(Month)])
    flow_vec = rep("Flow", nrow(summ))
    station_vec = rep(i, nrow(summ))
    # flow data to add onto stn_op_sched_new
    op_flow = data.frame(STATION_NUMBER=station_vec, 
                         DATA_TYPE=flow_vec, 
                         Year=summ$Year, 
                         Month_from=summ$min, 
                         Month_to=summ$max)
    stn_op_sched_new = rbind(stn_op_sched_new, op_flow)
    print(paste("stn_op_sched flow", i))
  }
  if (!(class(level_demo)[1]=="try-error")){
    summ = hy_monthly_levels(i) %>% group_by(Year) %>% 
      summarise(min = month.abb[min(Month)], max = month.abb[max(Month)]) 
    level_vec = rep("Water Level", nrow(summ))
    station_vec = rep(i, nrow(summ))
    #  water level data to add ontocretae stn_op_sched
    op_level = data.frame(STATION_NUMBER=station_vec, 
                          DATA_TYPE=level_vec, 
                          Year=summ$Year, 
                          Month_from=summ$min, 
                          Month_to=summ$max)
    stn_op_sched_new = rbind(stn_op_sched_new, op_level)
    print(paste("stn_op_sched level", i))
  }
}
################################################################################

########## stn_data_col ########################################################
# Note that the new table assigns operation schedule using the following rule:
# for stations that have data for all 12 months within a year->"Continuous"
# for stations that have data in 2 to 11 months within a year->"Seasonal"
# for station that only contain 1 month of data->"M"
# Inconsistencies between operation codes will be introduced later.
# Takes ~35 min to run
for (i in col_list){
  flow_demo = try(hy_monthly_flows(i), TRUE)
  level_demo = try(hy_monthly_levels(i), TRUE)
  #follow the order of flow - water level
  if (!(class(flow_demo)[1]=="try-error")){
    summ = hy_monthly_flows(i) %>% group_by(Year) %>% 
      summarise(all = length(unique(Month))) %>% mutate(operation = case_when(all==12~"Continuous",
                                                                              all>1 & all<=12~"Seasonal",
                                                                              all==1~"M"))
    #function to group operations
    #create an integer vector to group for stn_data_col
    op_col = c(1)
    if (!(length(summ$operation)==1)){
      k = 2
      j = 1
      prev_y = summ$Year[1]
      prev = summ$operation[1]
      while(k <=nrow(summ)){
        curr_y = summ$Year[k]
        curr = summ$operation[k]
        if (curr == prev && curr_y==(prev_y+1)){
          op_col=c(op_col, j)
          prev = curr
          prev_y = curr_y
          k = k+1
        }
        else{
          j = j+1
          op_col=c(op_col,j)
          prev = curr
          prev_y = curr_y
          k = k+1
        }
      }
    }
    #summarise summ to get columns needed for stn_data_col
    summ$op_col = op_col
    summ2 = summ %>% group_by(op_col) %>% 
      summarise(operation = operation[1],from = min(Year), to=max(Year)) %>% select(-op_col)
    col_station_vec = rep(i, nrow(summ2))
    col_flow_vec = rep("Flow", nrow(summ2))
    col_flow = data.frame(STATION_NUMBER = col_station_vec,
                          DATA_TYPE = col_flow_vec,
                          Year_from = summ2$from, 
                          Year_to = summ2$to,
                          OPERATION = summ2$operation)
    stn_data_col_new = rbind(stn_data_col_new, col_flow)
    print(paste("stn_data_col flow", i))
  }
  if (!(class(level_demo)[1]=="try-error")){
    summ = hy_monthly_levels(i) %>% group_by(Year) %>% 
      summarise(min = month.abb[min(Month)], max = month.abb[max(Month)], all = length(unique(Month))) %>% 
      mutate(operation = case_when(all==12~"Continuous",
                                   all>1 & all<=12~"Seasonal",
                                   all==1~"M"))
    #function to group operations
    #create an integer vector to group for stn_data_col
    op_col = c(1)
    if (!(length(summ$operation)==1)){
      k = 2
      j = 1
      prev_y = summ$Year[1]
      prev = summ$operation[1]
      while(k <=nrow(summ)){
        curr_y = summ$Year[k]
        curr = summ$operation[k]
        if (curr == prev && curr_y==(prev_y+1)){
          op_col=c(op_col, j)
          prev = curr
          prev_y = curr_y
          k = k+1
        }
        else{
          j = j+1
          op_col=c(op_col,j)
          prev = curr
          prev_y = curr_y
          k = k+1
        }
      }
    }
    #summarise summ to get columns needed for stn_data_col
    summ$op_col = op_col
    summ2 = summ %>% group_by(op_col) %>% 
      summarise(operation = operation[1],from = min(Year), to=max(Year)) %>% select(-op_col)
    col_station_vec = rep(i, nrow(summ2))
    col_level_vec = rep("Water Level", nrow(summ2))
    col_level = data.frame(STATION_NUMBER = col_station_vec,
                           DATA_TYPE = col_level_vec,
                           Year_from = summ2$from, 
                           Year_to = summ2$to,
                           OPERATION = summ2$operation)
    stn_data_col_new = rbind(stn_data_col_new, col_level)
    print(paste("stn_data_col level", i))
  }
}
################################################################################

########## stn_data_ran ########################################################
# takes 35 min~
for (i in ran_list){
  flow_demo = try(hy_monthly_flows(i), TRUE)
  level_demo = try(hy_monthly_levels(i), TRUE)
  #follow the order of water level-flow
  if (!(class(flow_demo)[1]=="try-error")){
    summ = hy_monthly_flows(i)
    record_length = length(unique(summ$Year))
    ran_flow = data.frame(STATION_NUMBER = i,
                          DATA_TYPE = "Flow",
                          Year_from = min(summ$Year),
                          Year_to = max(summ$Year),
                          RECORD_LENGTH = record_length)
    stn_data_ran_new = rbind(stn_data_ran_new, ran_flow)
    print(paste("stn_data_ran flow", i))
  }
  if (!(class(level_demo)[1]=="try-error")){
    summ = hy_monthly_levels(i)
    record_length = length(unique(summ$Year))
    ran_level = data.frame(STATION_NUMBER = i,
                           DATA_TYPE = "Water Level",
                           Year_from = min(summ$Year),
                           Year_to = max(summ$Year),
                           RECORD_LENGTH = record_length)
    stn_data_ran_new = rbind(stn_data_ran_new, ran_level)
    print(paste("stn_data_ran level", i))
  }
}
################################################################################
write.csv(stn_op_sched_new, file = "./Output/stn_op_sched_new.csv", row.names=FALSE)
write.csv(stn_data_col_new, file = "./Output/stn_data_col_new.csv", row.names=FALSE)
write.csv(stn_data_ran_new, file = "./Output/stn_data_ran_new.csv", row.names=FALSE)
################################################################################


#### Reformat the operation tables to compare #################################
# (1) Station operation schedule
# STATION_NUMBER / DATA_TYPE / Year / Month_from / Month_to
# No changes to format

# (2) Station data collection
# STATION_NUMBER / DATA_TYPE / Year_from / Year_to / MEASUREMENT / OPERATION
# Drop extra columns
stn_data_col_update <- select(stn_data_col, -one_of("MEASUREMENT"))
# Expand Year_from / Year_to into one Year column
stn_data_col_update$Year <- mapply(seq, stn_data_col_update$Year_from, stn_data_col_update$Year_to, SIMPLIFY=FALSE)
stn_data_col_update <- stn_data_col_update %>% 
  unnest(Year) %>% 
  select(-Year_from,-Year_to)
stn_data_col_new_update = stn_data_col_new
stn_data_col_new_update$Year <- mapply(seq, stn_data_col_new_update$Year_from, stn_data_col_new_update$Year_to, SIMPLIFY=FALSE)
stn_data_col_new_update <- stn_data_col_new_update %>% 
  unnest(Year) %>% 
  select(-Year_from,-Year_to)

#(3) Station data range
# None of those changes really applies to this table so keep unchanged.

################################################################################



######## section2 Contrast between tables ######################################
# stn_op_sched #################################################################
# HELPER FUNCTIONS
# hyincons_check() will only be processed when a station has flow/water level data recorded in the 
# table but not in hydat. Function store the inconsistency in a data frame f and returns "incons".
# hyincons_check: character of("Flow" or "Water Level") -> character of ("incons")
hyincons_check <- function(type){
  bad_yrs <- (yeardat %>% filter(DATA_TYPE == type))$Year
  f <<- data.frame(STATION_NUMBER =  rep(i, length(bad_yrs)),
                   DATA_TYPE = rep(type, length(bad_yrs)),
                   Year = bad_yrs,
                   Reason = rep(paste("Hydat does not have",type, "record for", i),
                                         length(bad_yrs)))
  return("incons")
}
################################################################################
# diff_check() will only be processed when a station has flow/water level data recorded in the
# table and hydat. Function store any inconsistencies in a data frame f and returns "diff" if
# inconsistencies exist, if not returns "no diff".
# diff_check: character of ("Flow" or "Water Level") dataframe -> character of ("diff" or "no diff")
diff_check <- function(type,table){
  old_ref = (yeardat %>% filter(DATA_TYPE==type))$Year
  new_ref = (table %>% filter(STATION_NUMBER==i&DATA_TYPE==type))$Year
  if (any(!(old_ref==new_ref))){
    ind1 = old_ref[!(old_ref %in% new_ref)]
    ind2 = new_ref[!(new_ref %in% old_ref)]
    #seek for differences in ind1 ad ind2
    f <<- data.frame(STATION_NUMBER = rep(i, length(ind1)+length(ind2)),
                     DATA_TYPE = rep(type, length(ind1)+length(ind2)),
                     Year = c(ind1, ind2),
                     Reason = c(rep("Missing in Hydat", length(ind1)), 
                                rep("Missing in stn_op_sched", length(ind2))))
    return("diff")
  }
  else{return("no diff")}
}
################################################################################
# nodata_check() will only be processed when a station does not have flow/water level data
# recorded in the table but hydat does. Function store any inconsistencies in a data
# f and returns "missing type".
# nodata_check(): character of ("Flow" or "Water Level") dataframe -> character of("missing type")
nodata_check <- function(type, table){
    norecord_yr = unique((table %>% filter(STATION_NUMBER==i &DATA_TYPE==type))[["Year"]])
    f <<- data.frame(STATION_NUMBER = rep(i, length(norecord_yr)),
                     DATA_TYPE = rep(type, length(norecord_yr)),
                     Year = norecord_yr,
                     Reason = rep(paste("table does not have", type, "data for",i), length(norecord_yr)))
    return("missing type")
}
################################################################################
# error_op iterate through all unique stations in stn_op_sched(and because of the way
# we build stn_op_sched_new, all unique stations in stn_op_sched_new as well);
# using three ifelse statements, we classify all possible outcomes into four conditions,
# and assign individual flow/level indicator variable to each data types.
# Depends on the indicator, data frame will be augmented or remain unchanged.
# || STATION_NUMBER || DATA_TYPE || Year || Reason ||
# Takes ~30 min to run
error_op = data.frame()
for (i in sched_list){
  print(i)
  yeardat = stn_op_sched %>% filter(STATION_NUMBER == i)
  try_err = class(try(hy_monthly_flows(i), TRUE))[1]
  flow_indicator = ifelse("Flow" %in% yeardat$DATA_TYPE, 
                        ifelse(try_err=="try-error", hyincons_check("Flow"),diff_check("Flow", stn_op_sched_new)),
                        ifelse(!try_err=="try-error", nodata_check("Flow", stn_op_sched_new), "no data"))
  if (flow_indicator=="incons"|flow_indicator=="diff"|flow_indicator=="missing type"){
    error_op = rbind(error_op, f)
  }
  try_err = class(try(hy_monthly_levels(i), TRUE))[1]
  level_indicator=ifelse("Water Level" %in% yeardat$DATA_TYPE, 
                         ifelse(try_err=="try-error", hyincons_check("Water Level"),diff_check("Water Level", stn_op_sched_new)),
                         ifelse(!try_err=="try-error", nodata_check("Water Level", stn_op_sched_new), "no data"))
  if (level_indicator=="incons"|level_indicator=="diff"|level_indicator=="missing type"){
    error_op = rbind(error_op, f)
  }
}
################################################################################
# Output the csv
error_op = read.csv(f)
write.csv(error_op, file = "error_op.csv", row.names=FALSE) 




###############################################################################
operation_check<- function(type, table){
  old_ref = (yeardat %>% filter(DATA_TYPE==type))$Year
  new_ref = (table %>% filter(STATION_NUMBER==i,DATA_TYPE==type))$Year
  if (any(!(old_ref==new_ref))){
    ind1 = old_ref[!(old_ref %in% new_ref)]
    ind2 = new_ref[!(new_ref %in% old_ref)]
    #seek for differences in ind1 ad ind2 
    f <<- data.frame(STATION_NUMBER = rep(i, length(ind1)+length(ind2)),
                     DATA_TYPE = rep(type, length(ind1)+length(ind2)),
                     Year = c(ind1, ind2),
                     Reason = c(rep("Missing in Hydat", length(ind1)), 
                                rep("Missing in stn_data_col", length(ind2))))
  }else{
    f <<- data.frame()
  }
  same_ref = old_ref[old_ref %in% new_ref]
  for (y in same_ref){
    op1 = (yeardat %>% filter(DATA_TYPE==type, Year==y))$OPERATION
    op2 = (table %>% filter(STATION_NUMBER==i , DATA_TYPE==type , Year==y))$OPERATION
    if (!(op1==op2)){
      f <<- rbind(f, data.frame(STATION_NUMBER = i,
                                DATA_TYPE = type,
                                Year = y,
                                Reason = paste("Expected:", op2, "Got:", op1)))
    }
  }
  return(ifelse(any(!(old_ref==new_ref)), "diff", "no diff"))
}
################################################################################

# stn_data_col #################################################################
#error_col iterate through col_list and record all inconsistencies in it.
# In addition to the four types introduced above, we introduce one extra
# type which is the difference in operation code.
# Tkaes approximately 50 min~
# || STATION_NUMBER || DATA_TYPE || Year || Reason ||
error_col = data.frame()
for (i in col_list){
  print(i)
  yeardat = stn_data_col_update %>% filter(STATION_NUMBER == i)
  try_err = class(try(hy_monthly_flows(i), TRUE))[1]
  flow_indicator=ifelse("Flow" %in% yeardat$DATA_TYPE, 
                        ifelse(try_err=="try-error", hyincons_check("Flow"),operation_check("Flow", stn_data_col_new_update)),
                        ifelse(!try_err=="try-error", nodata_check("Flow", stn_data_col_new_update), "no data"))
  if (flow_indicator=="incons"|flow_indicator=="diff"|flow_indicator=="missing type"){
    error_col = rbind(error_col, f)
  }
  try_err = class(try(hy_monthly_levels(i), TRUE))[1]
  level_indicator=ifelse("Water Level" %in% yeardat$DATA_TYPE, 
                         ifelse(try_err=="try-error", hyincons_check("Water Level"),operation_check("Water Level", stn_data_col_new_update)),
                         ifelse(!try_err=="try-error", nodata_check("Water Level", stn_data_col_new_update), "no data"))
  if (level_indicator=="incons"|level_indicator=="diff"|level_indicator=="missing type"){
    error_col = rbind(error_col, f)
  }
}

error_col = unique(error_col)
########################################################
write.csv(error_col, file = "error_col.csv", row.names=FALSE) 
################################################################################



################################################################################
# stn_data_ran
# Initially there were codes that iterate through all stations like the two tables
# above. However, the code returns an empty data frame after executing. Therefore, 
# the following cides have been created as a proof that stn_data_ran_new, the 
# table pulled out from hydat, has all the same contents to stn_data_ran within
# the data_types Q and H. A count variable equal to zero was returned when 
# after executing each of the two loops, justifies the assumption that no inconsistencies
# occur between two tables.
# each takes ~ 2mins
count = 0
for (i in ran_list){
  print(i)
  yeardat = stn_data_ran %>% filter(STATION_NUMBER == i)
  if ("Q" %in% yeardat$DATA_TYPE){
    new = stn_data_ran_new %>% filter(STATION_NUMBER==i&DATA_TYPE=="Flow") %>% select("Year_from", "Year_to", "RECORD_LENGTH")
    old = stn_data_ran %>% filter(STATION_NUMBER==i&DATA_TYPE=="Q") %>% select("Year_from", "Year_to", "RECORD_LENGTH")
    if (!(all(new==old))){
      count = count +1
    }else{print("pass")}
  }
  if ("H" %in% yeardat$DATA_TYPE){
    new = stn_data_ran_new %>% filter(STATION_NUMBER==i&DATA_TYPE=="Water Level") %>% select("Year_from", "Year_to", "RECORD_LENGTH")
    old = stn_data_ran %>% filter(STATION_NUMBER==i&DATA_TYPE=="H") %>% select("Year_from", "Year_to", "RECORD_LENGTH")
    if (!(all(new==old))){
      count = count +1
    }else{print("pass")}
  }
}
count = 0
for (i in ran_list){
  print(i)
  yeardat = stn_data_ran_new %>% filter(STATION_NUMBER == i)
  if ("Flow" %in% yeardat$DATA_TYPE){
    new = stn_data_ran_new %>% filter(STATION_NUMBER==i&DATA_TYPE=="Flow") %>% select("Year_from", "Year_to", "RECORD_LENGTH")
    old = stn_data_ran %>% filter(STATION_NUMBER==i&DATA_TYPE=="Q") %>% select("Year_from", "Year_to", "RECORD_LENGTH")
    if (!(all(new==old))){
      count = count +1
    }else{print("pass")}
  }
  if ("Water Level" %in% yeardat$DATA_TYPE){
    new = stn_data_ran_new %>% filter(STATION_NUMBER==i&DATA_TYPE=="Water Level") %>% select("Year_from", "Year_to", "RECORD_LENGTH")
    old = stn_data_ran %>% filter(STATION_NUMBER==i&DATA_TYPE=="H") %>% select("Year_from", "Year_to", "RECORD_LENGTH")
    if (!(all(new==old))){
      count = count +1
    }else{print("pass")}
  }
}
################################################################################
# For the sake of future usage, the code is being stored independently. 
# In cases of any changes that might have been made, execute the code for potential
# inconsistencies.
################################################################################

########## Summarize ###########################################################
# group_by the station, filter by the error type and summ
# stn_op_sched
p1 = error_op %>% filter(grepl("in Hydat", Reason), Year>=2010) %>% group_by(STATION_NUMBER) %>% summarise(len = length(STATION_NUMBER))
p2 = error_op %>% filter(grepl("in Hydat", Reason), Year< 2010) %>% group_by(STATION_NUMBER) %>% summarise(len = length(STATION_NUMBER))
p3 = error_op %>% filter(grepl("table does not have", Reason), Year>=2010) %>% group_by(STATION_NUMBER) %>% summarise(len = length(STATION_NUMBER))
p4 = error_op %>% filter(grepl("table does not have", Reason), Year<2010) %>% group_by(STATION_NUMBER) %>% summarise(len = length(STATION_NUMBER))
p5 = error_op %>% filter(grepl("in stn_op_sched", Reason), Year>=2010) %>% group_by(STATION_NUMBER) %>% summarise(len = length(STATION_NUMBER))
p6 = error_op %>% filter(grepl("in stn_op_sched", Reason), Year<2010) %>% group_by(STATION_NUMBER) %>% summarise(len = length(STATION_NUMBER))
p7 = error_op %>% filter(grepl("Hydat does not have", Reason), Year>=2010) %>% group_by(STATION_NUMBER) %>% summarise(len = length(STATION_NUMBER))
p8 = error_op %>% filter(grepl("Hydat does not have", Reason), Year<2010) %>% group_by(STATION_NUMBER) %>% summarise(len = length(STATION_NUMBER))

# take some cesi code and map the problematic stations 
# separate missing in hydat that's before and after 2010