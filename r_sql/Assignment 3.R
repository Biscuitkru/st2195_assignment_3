library(DBI)
library(dplyr)

airports <- read.csv("airports.csv", header = TRUE)
carriers <- read.csv("carriers.csv", header = TRUE)
planes <- read.csv("plane-data.csv", header = TRUE)
zero <- read.csv("2000.csv", header = TRUE)
one <- read.csv("2001.csv", header = TRUE)
two <- read.csv("2002.csv", header = TRUE)
three <- read.csv("2003.csv", header = TRUE)
four <- read.csv("2004.csv", header = TRUE)
five <- read.csv("2005.csv", header = TRUE)
ontime <- rbind(zero, one, two, three, four, five)
#or
for(i in c(2000:2005)) {
  filename <- paste0("./Block 3/", i , ".csv")
  print(paste("Processing:", i))
  ontime <- read.csv(filename, header = TRUE)
  if (i == 2000) {
  dbWriteTable(conn, "ontime", ontime)
  } else {
  dbWriteTable(conn, "ontime", ontime, append = TRUE)
 }
}

conn <- dbConnect(RSQLite::SQLite(), "airline2.db")

dbWriteTable(conn, "Airports", airports)
dbWriteTable(conn, "Carriers", carriers)
dbWriteTable(conn, "Planes", planes)
dbWriteTable(conn, "Ontime", ontime)
dbListTables(conn)

# Q1 - Which plane model has the lowest associated average departure delay
# (excluding cancelled and diverted flights)?
q1 <- dbGetQuery(conn, "SELECT model, AVG(DepDelay) as avg_depdelay FROM Planes,
                 ontime WHERE DepDelay > 0 AND Planes.tailnum = ontime.tailnum AND ontime.cancelled = 0
                 AND ontime.Diverted = 0 GROUP BY model ORDER BY avg_depdelay ASC")

print(paste(q1[1, "model"], "has the lowest associated average departure delay."))
# "737-2Y5 has the lowest associated average departure delay."
## dplyr
q1_dplyr <- ontime %>%
  filter(Cancelled==0, Diverted==0, DepDelay>0) %>%
  inner_join(planes, by=c('TailNum' = 'tailnum')) %>%
  group_by(model) %>%
  summarize(avg_DepDelay=mean(DepDelay, na.rm = TRUE)) %>%
  arrange(avg_DepDelay)

print(paste(q1_dplyr[1, "model"], "has the lowest associated average departure delay."))
# "737-2Y5 has the lowest associated average departure delay."

# Q2 - Which city has the highest number of inbound flights(excluding cancelled flights)?
q2 <- dbGetQuery(conn, "SELECT airports.city as city, COUNT(Dest) AS 'Inbound Flights'
                 FROM airports, ontime
                 WHERE Cancelled=0
                 AND ontime.Dest = airports.iata
                 GROUP BY Dest
                 ORDER BY COUNT(Dest) DESC")

print(paste(q2[1, "city"], "has the highest number of inbound flights."))
# "Chicago has the highest number of inbound flights."

dbGetQuery(conn, "SELECT
           airports.city,
           COUNT(*) AS flights
           FROM ontime JOIN airports ON ontime.Dest = airports.iata
           GROUP BY airports.city
           ORDER BY flights DESC LIMIT 10")

q2

q2_dplyr <- ontime %>%
  filter(Cancelled==0) %>%
  inner_join(airports, by=c('Dest' = 'iata')) %>%
  group_by(city) %>%
  summarize(total=n()) %>%
  arrange(desc(total))
print(head(q2_dplyr, 1))

# Q3 - Which carrier has the highest number of cancelled flights?
q3 <- dbGetQuery(conn, "SELECT Description AS 'Carrier', COUNT(Cancelled) AS 'Cancelled Flights'
                 FROM carriers, ontime 
                 WHERE ontime.UniqueCarrier = carriers.Code AND Cancelled = 1
                 GROUP BY Description ORDER BY COUNT(Cancelled) DESC")

print(paste(q3[1, "Carrier"], "has the highest number of cancelled flights."))
# "Delta Air Lines Inc. has the highest number of cancelled flights."

q3_dplyr <- ontime %>%
  filter(Cancelled==1) %>%
  inner_join(carriers, by=c('UniqueCarrier'='Code')) %>%
  group_by(carrier = Description) %>%
  summarize(Cancelled=n()) %>%
  arrange(desc(Cancelled)) 

print(paste(q3_dplyr[1, "carrier"], "has the highest number of cancelled flights."))
# "Delta Air Lines Inc. has the highest number of cancelled flights."

# Q4 - Which carrier has the highest number of cancelled flights, relative to 
# their number of total flights? Note: ,3) Means rounding it to 3 decimal places

q4 <- dbGetQuery(conn, "SELECT Description AS 'Carrier', ROUND(SUM(Cancelled)*100.0/ 
                 COUNT(UniqueCarrier), 3) AS 'Cancelled Flights to Inbound Flights Ratio'
                 FROM ontime, carriers
                 WHERE ontime.UniqueCarrier = carriers.Code
                 GROUP BY Description
                 ORDER BY ROUND(SUM(Cancelled)*100.0/COUNT(UniqueCarrier), 1) DESC")
# or 
q4 <- dbGetQuery(conn, 
                 "SELECT carriers.Description as Carrier, avg(ontime.Cancelled)*100
                  as Cancelled_ratio FROM ontime JOIN carriers ON ontime.UniqueCarrier=carriers.Code
                  GROUP BY carrier
                  ORDER BY Cancelled_ratio DESC")

print(paste(q4[1, "Carrier"], "has the highest number of cancelled flights relative to their number of total flights."))
# "American Eagle Airlines Inc. has the highest number of cancelled flights relative to their number of total flights."

q4_dplyr <- inner_join(ontime, carriers, by = c("UniqueCarrier" = "Code")) %>%
  rename(carrier = Description) %>%
  group_by(carrier) %>%
  summarise(ratio = mean(Cancelled, na.rm=TRUE)) %>%
  arrange(desc(ratio))

print(head(q4_dplyr, 1))


# 2 sub-queries method
# (i) number of cancelled flights - numerator;
# (ii) number of flights - denominator
# join the 2 sub-queries
# CAST function is needed to convert INTEGER(in counts) to FLOAT

q4.2 <- dbGetQuery(conn, "SELECT q1.carrier AS carrier, (CAST(q1.numerator AS FLOAT)/ CAST(q2.denominator AS FLOAT)) AS ratio
                   FROM
                   (
                   SELECT carriers.Description AS carrier, COUNT(*)*100 AS numerator
                   FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
                   WHERE ontime.Cancelled = 1
                   GROUP BY carriers.Description
                   ) AS q1 JOIN
                   (
                   SELECT carriers.Description AS carrier, COUNT(*) AS denominator
                   FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
                   GROUP BY carriers.Description
                   )
                   AS q2 using(carrier)
                   ORDER BY ratio DESC")

print(paste(q4.2[1, "carrier"], "has the highest number of cancelled flights relative to their number of total flights."))
# "American Eagle Airlines Inc. has the highest number of cancelled flights relative to their number of total flights."
q4.2a <- inner_join(ontime, carriers, by=c("UniqueCarrier"="Code")) %>%
  filter(Cancelled==1) %>%
  group_by(Description) %>%
  summarize(numerator = n()) %>%
  rename(carrier = Description)

q4.2b <- inner_join(ontime, carriers, by = c("UniqueCarrier" = "Code")) %>%
  group_by(Description) %>%
  summarize(denominator = n()) %>%
  rename(carrier = Description)

q4.2_dplyr <- inner_join(q4a, q4b, by="carrier") %>%
  mutate_if(is.integer, as.double) %>%
  mutate(numerator = as.double(numerator)) %>%
  mutate(denominator = as.double(denominator)) %>%
  mutate(ratio = numerator/denominator) %>% 
  select(carrier, ratio) %>%
  arrange(desc(ratio))

print(head(q4_dplyr, 1))

dbDisconnect(conn)
