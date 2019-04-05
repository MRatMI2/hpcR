library(sparklyr)
library(nycflights13)
library(dplyr)
# spark_install(version = "2.1.0")
# https://www.programcreek.com/2018/11/install-spark-in-local-mode-on-ubuntu/

sc <- spark_connect(master = "local")

flights_tbl <- copy_to(sc, flights, "flights")
src_tbls(sc)

dim(flights)
dim(flights_tbl)

filter(flights, month == 1)
filter(flights_tbl, month == 1)

# nie zawsze sparklyr dziala tak jak tego oczekujemy ------------------------

group_by(flights, month) %>% 
  summarise(n_flights = length(month))

group_by(flights_tbl, month) %>% 
  summarise(n_flights = length(month))

# idiomy z dplyr z reguly dzialaja -------------------------------------

group_by(flights_tbl, month) %>% 
  summarise(n_flights = n())


