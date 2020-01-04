"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("CONNECT-stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)
table = app.Table(
    "stations_total",
    default=int
)


@app.agent(topic)
async def stationevent(stationevents):
    async for station in stationevents:
        color = ""
        if(station.red):
            color = "red"
        elif (station.blue):
            color = "blue"
        else:
            color = "green"

        await out_topic.send(key=str(station.station_id), value=TransformedStation(station_id=station.station_id,
                                                                       station_name=station.station_name,
                                                                       order=station.order,
                                                                       line=color))
        pass

if __name__ == "__main__":
    app.main()
