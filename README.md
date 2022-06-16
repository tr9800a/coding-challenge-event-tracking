# Challenge

## What
In order to gain insights about our fleet operating in the Hamburg and Hanover areas, we'd like to measure how many kilometers each vehicle drove per business day, as well as the number of events sent by each vehicle's computer.
One business day is defined as the period of time between start of operations and end of operations.

- Monday    05:00 - Tuesday    01:00 => Business Day Monday
- Tuesday   05:00 - Wednesday  01:00 => Business Day Tuesday
- Wednesday 05:00 - Thursday   01:00 => Business Day Wednesday
- Thursday  05:00 - Friday     05:00 => Business Day Thursday
- Friday    05:00 - Saturday   05:00 => Business Day Friday
- Saturday  05:00 - Sunday     00:00 => Business Day Saturday
- Sunday    00:00 - Sunday     06:00 => Business Day Sunday
- Sunday    10:00 - Monday     01:00 => Business Day Sunday



## Requirements
**Your job**: Write a batch job which computes, for each car and for each business day, the driven km as well as the number of events sent **by the vehicle**.

- The job must be launch via a simple `docker run -v input:input -v output:output challenge`.
- The output is written as csv to the `output` directory.
- Using Python is strongly recommended, but you may use the programming language and data processing framework you feel most comfortable with.
- This batch job will eventually be deployed in production. The job will run at two different levels: 
	* Hourly schedule: Process a couple of hours worth of data (~MBs).
	* Daily schedule: Process days, months, even years worth of data (~GBs and up).


## Data set

The data set contains raw geo-positions which indicate the location of a car for a given point in time.

```
"uuid","vehicle_id","created_timestamp","location_raw_lat","location_raw_lon"
"3e5b4e8f-4953-4258-afca-5305329706f1","vehicle-hamburg-118","2019-05-31 08:16:02+05","53.6088237","9.9562622"
"b5579389-458d-49a8-997e-d9af757a249f","vehicle-hamburg-77","2019-05-31 08:16:05+05","53.5501667","9.9716466"
"af939570-0dca-4ef0-b5b8-f9ed23c7a7d0","vehicle-hamburg-118","2019-05-31 08:16:06+05","53.6092794","9.9564227"
"4d24d9fc-9122-440c-81b8-4784cbf79107","vehicle-hamburg-77","2019-05-31 08:16:08+05","53.5503285","9.9711433"
"f115327d-bf28-466b-8150-75e0af7a0fe0","vehicle-hamburg-118","2019-05-31 08:16:10+05","53.6097198","9.9566445"
"3201b79a-cc38-4186-aebb-6674ac9f5d40","vehicle-hamburg-77","2019-05-31 08:16:12+05","53.5507136","9.9704620"
"23ad840f-4821-4bc8-bdfd-58a7bc0dc53d","vehicle-hamburg-118","2019-05-31 08:16:14+05","53.6101545","9.9568781"
...
...
...
```

- **uuid**: Event identifier added by the **vehicle's computer**
- **vehicle_id**: id of the vehicle
- **location_raw_lat**: latitude part of geo position
- **location_raw_lon**: longitude part of geo position
- **created_timestamp**: timestamp when the event occurred

The resulting table should contain the aggregated information in the following format:
```
vehicle_id, date, businessday_name, km_driven, num_events
"vehicle-city-123", "2019-06-15", "Monday", 104.32, 302
```

# Submission

## The Dataset
The timestamp is set to a UTC+5 timezone, suggesting I subtract three hours from each timestamp to move it into German UTC+2. Doing a count of events per hour per calendar day, I found a pattern of reduced activity (under 50 events per hour per day, versus appx. 350 average overall) during the 02:00-05:00 UTC+2 window of time on days where that window was also listed as outside of operational hours. With this supporting evidence I adjusted the timezone and proceeded.

I extracted date, time, and day of week information from the timestamp into their own columns for ease of processing the data. With this, I created a library with a sequential key containing the calendar day, beginning time, ending time, and business day to which that time block belonged. I then created a business day column filled with None. Using the library, a function to aggregate the filters, and a for loop with .loc, I filled all applicable values with business day names. Any remaining None values were dropped, as they were outside of business hours and of no use to the described purpose.

There were 60k duplicate rows as well -- these were dropped

Finally, there were 1058 events with location reported as (0.0, 0.0). I dropped these as they also serve no benefit to the goal of tracking vehicle activities.

## vehicle_tracker.py
My goal with this process was legible efficiency. To that end, I wrote a pair of classes, the first to handle the individual information for each individual vehicle, and the second to direct the data, instantiating vehicles and recording end of day totals as needed. 

Vehicles would maintain a record of the last event location and time, as well as a running tally of events and distance. Distance was calculated using Haversine distance between the stored location and the newly provided information.

Fleet would process the input data. Its first check was if the business day had changed; if so, it would initiate end of day, recording the data from each vehicle in fleet.vehicles into a dataframe, and then clearing fleet.vehicles and deleting each vehicle object. If business day matches between stored and input information, then it would proceed to check if the vehicle already exists. If the vehicle does not exist, then it would be created with an initial state set by the input data. If it does exist, then it will be updated.

There were some minor hiccups in the development of these classes; most notable was realizing that a library was more useful than a list to store active fleet vehicles (fleet.vehicles), as I could check the existence of the vehicle_id among the keys, and then use that information to call up the Vehicle() object to update its information.

Finally, to iterate through a dataframe the size of the input data, I wanted to be very conscious of the processing time of a function like iterrows. For the needs of this job, zip seemed like the best fit for maintaining the data but processing in a timely manner.

Final km_driven tallies were rounded to two decimal places for legibility and ease of handling. A higher degree of precision could be somewhat misleading considering the approximate error rate of 0.5% with Haversine distances.