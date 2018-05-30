# Ottawa Learnathon 2018
# Using Kaggle competition datasets
# Competition (closed): Recruit Restaurant Visitor Forecasting
# Created by: C. Mimeault with help from others :)
# Created on: May 26, 2018

# Code to merge the air reservation and visit datasets

# Import required libraries
library(tidyverse)
library(lubridate)
library(ggplot2)

# Read raw csv files
reservations <-
  read_csv(
    "~/projects/learnathon_2018/recruit-restaurant-visitor-forecasting/air_reserve.csv"
  )
visits <-
  read_csv(
    "~/projects/learnathon_2018/recruit-restaurant-visitor-forecasting/air_visit_data.csv"
  )

data <- reservations %>%
  # Extract date from datetime and save in new column (planned_date)
  mutate(planned_date = date(visit_datetime)) %>%
  # Select columns of interest in reservation dataset
  select(air_store_id, planned_date, reserve_visitors) %>%
  # Group data by store and date
  group_by(air_store_id, planned_date) %>%
  # Sum the number of visitors at each restaurant and date
  summarize(expected_number_visitors = sum(reserve_visitors)) %>%
  # Excluding the few instances in which the number of expected visitors are errors
  filter(expected_number_visitors < 400) %>%
  # Join the reservation dataset with the visit dataset based on restaurant and dates
  left_join(visits,
            by = c("air_store_id" = "air_store_id", "planned_date" = "visit_date"))

# Plot data and save image
ggplot(data, mapping = aes(x = expected_number_visitors, y = visitors)) + geom_point() + geom_smooth(method =
                                                                                                   lm) + xlim(NA, 175)
ggsave(
  "~/projects/learnathon_2018/kaggle-restaurant-visitor-forecasting/expected_vs_actual_visitors.png"
)