from google.transit import gtfs_realtime_pb2
import pandas as pd 
import time
import requests

def get_feed():
    response = requests.get("https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/vehicle/vehiclepositions.pb")

    # Read the feed 
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed 

def process_feed(feed):
    data = []
    for entity in feed.entity:
         vehicle = entity.vehicle
         data.append({
            "timestamp": feed.header.timestamp,
            "vehicle_id": vehicle.vehicle.id,
            "latitude": vehicle.position.latitude,
            "longitude": vehicle.position.longitude,
            "speed": vehicle.position.speed if vehicle.position.HasField("speed") else None,
            "bearing": vehicle.position.bearing if vehicle.position.HasField("bearing") else None,
            "trip_id": vehicle.trip.trip_id if vehicle.trip.HasField("trip_id") else None,
            "route_id": vehicle.trip.route_id if vehicle.trip.HasField("route_id") else None
        })

    return pd.DataFrame(data)

# Current time plus number of hours * 3600 
end_time = time.time() + 60
csv_path = 'log.csv'
wait_time = 1

prev_time = None
# Run until hit specified end time 
while time.time() < end_time:
     # Read the current feed from the PB 
     feed = get_feed()

     if feed.header.timestamp != prev_time: 
          # Process feed into a DF and add it to the CSV 
          output = process_feed(feed)
          output.to_csv(csv_path, mode="a", index=False, header=not pd.io.common.file_exists(csv_path))
          prev_time = feed.header.timestamp 

     # Wait a lil bit
     time.sleep(wait_time)
     