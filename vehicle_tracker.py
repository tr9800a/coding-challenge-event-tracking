from datetime import datetime, timedelta
import pandas as pd
from haversine import haversine

# Import and prepare vehicle event data, adjusting time back three time zones (UTC+5 --> UTC+2) creating relevant date and time columns
vehicle_data = pd.read_csv('./input/location_updates.csv.gz', parse_dates = ['created_timestamp'])
vehicle_data['created_timestamp'] -= timedelta(hours = 3)
vehicle_data['date'] = vehicle_data.created_timestamp.dt.date
vehicle_data['day'] = vehicle_data.created_timestamp.dt.day_name()
vehicle_data['time'] = vehicle_data.created_timestamp.dt.time

# Drop events with (0.0, 0.0) latitude and longitude, as nothing of value can be extracted
vehicle_data.drop(vehicle_data[vehicle_data.location_raw_lon == 0.0].index, inplace = True)
# Drop duplicate events
vehicle_data.drop_duplicates(inplace = True)

# Library of calendar day, start time, end time, business day to iterate through and label events
business_days = {0:['Monday', '05:00:00', '23:59:59', 'Monday'],
                    1:['Tuesday', '00:00:00', '01:00:00', 'Monday'],
                    2:['Tuesday', '05:00:00', '23:59:59', 'Tuesday'],
                    3:['Wednesday', '00:00:00', '01:00:00', 'Tuesday'],
                    4:['Wednesday', '05:00:00', '23:59:59', 'Wednesday'],
                    5:['Thursday', '00:00:00', '01:00:00', 'Wednesday'],
                    6:['Thursday', '05:00:00', '23:59:59', 'Thursday'],
                    7:['Friday', '00:00:00', '04:59:59', 'Thursday'],
                    8:['Friday', '05:00:00', '23:59:59', 'Friday'],
                    9:['Saturday', '00:00:00', '04:59:59', 'Friday'],
                    10:['Saturday', '05:00:00', '23:59:59', 'Saturday'],
                    11:['Sunday', '00:00:00', '06:00:00', 'Sunday'],
                    12:['Sunday', '10:00:00', '23:59:59', 'Sunday'],
                    13:['Monday', '00:00:00', '01:00:00', 'Sunday']}

# Function to extract the boolean masks and business day name from the library above
def business_hours_filters(i):
    daymask = vehicle_data['day'] == business_days[i][0]
    t1 = datetime.strptime(business_days[i][1], '%H:%M:%S').time()
    t2 = datetime.strptime(business_days[i][2], '%H:%M:%S').time()
    openmask = vehicle_data['time'] >= t1
    closemask = vehicle_data['time'] <= t2
    business_day_name = business_days[i][3]
    filters = daymask & openmask & closemask
    return filters, business_day_name

# Iterate through the library of business_day parameters, using the above function to create
# masks and then apply the correct business day name to each event. Drop events outside of
# business hours (events retaining None/NaN value) 
vehicle_data['business_day'] = None
for i in business_days:
    filters, business_day_name = business_hours_filters(i)
    vehicle_data.loc[filters, 'business_day'] = business_day_name
vehicle_data.dropna(inplace = True)

# A pair of classes to process the input data. A vehicle class to manage the history of each 
# vehicle, and then a fleet class to direct data and manage the recording of day end totals.
class Fleet:
    """Manages business day functions and all vehicles currently reporting"""
    def __init__(self):
        self.vehicles = {}
        self.last_event = None
        self.business_day = None

    def __repr__(self):
        return "Fleet('{}','{}','{}')".format(self.vehicles, self.last_event, self.business_day)

    def __str__(self):
        return "'{}' - '{}' - '{}'".format(self.vehicles, self.last_event, self.business_day)

    def add_vehicle(self, id, time, date, lat, long):
        """Create library entry with vehicle id as key and Vehicle class object as value"""
        v_id = id
        v_id = Vehicle(id, time, date, lat, long)
        self.vehicles[id] = v_id
    
    def process_events(self, id, biz_day, time, date, lat, long):
        """Parse row content to decide action. If first entry of the day, assign business_day value.
        If stored business day does not match row, then end previous day and start anew. If vehicle id
        is in fleet, update info. If id is not in fleet, add."""
        if self.business_day is None:
            self.business_day = biz_day
        if self.business_day != biz_day:
            self.end_of_day(biz_day)
        if id in self.vehicles:
            self.vehicles[id].reporting(time, lat, long)
        else:
            self.add_vehicle(id, time, date, lat, long)

    def end_of_day(self, biz_day):
        """For each active vehicle in fleet, create list of needed information, append to output dataframe,
        and delete vehicle object. Once complete, set new business day and clear fleet list."""
        for id in self.vehicles:
            vehicle = self.vehicles[id]
            info = []
            info.append(vehicle.id)
            info.append(vehicle.date)
            info.append(self.business_day)
            info.append(round(vehicle.distance,2))
            info.append(vehicle.event_count)
            output.append(info)
            vehicle.__del__()
        self.business_day = biz_day
        self.vehicles = {}

class Vehicle:
    """A class to track each vehicle's behaviors"""
    def __init__(self, id, time, date, lat, long):
        self.id = id.strip()
        self.last_event = time
        self.date = date
        self.event_count = 1
        self.lat = lat
        self.long = long
        self.distance = 0

    def __repr__(self):
        return "Vehicle('{}', {}, {}, {}, {}, {})".format(self.id, self.last_event, self.date, self.event_count, self.lat_long, self.distance)

    def __str__(self):
        return '{} - {} - {} - {} - {}'.format(self.id, self.date, Fleet.business_day, self.distance, self.event_count)

    def __del__(self):
        return ''

    @property
    def lat_long(self):
        return '({}, {})'.format(self.lat, self.long)
    
    def reporting(self, time, lat, long):
        """Calculate haversine distance from stored to new location. 
        Update running totals for event count and distance."""
        self.distance += haversine((self.lat, self.long), (lat, long))
        self.lat = lat
        self.long = long
        self.last_event = time
        self.event_count += 1
    

output = []
fleet = Fleet()

# Zip through all relevant information from input.csv, feeding it to the fleet.process_events
# to sort through necessary actions. When finished, run end of day again to clear final day's info.
for row in zip(vehicle_data['vehicle_id'],
                        vehicle_data['business_day'],
                        vehicle_data['time'],
                        vehicle_data['date'],
                        vehicle_data['location_raw_lat'],
                        vehicle_data['location_raw_lon']):
    fleet.process_events(row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5])
fleet.end_of_day(None)
output = pd.DataFrame(output, columns = ['vehicle_id', 
                                        'date', 
                                        'businessday_name', 
                                        'km_driven', 
                                        'num_events'])
output.to_csv('./output/events.csv', index = False)