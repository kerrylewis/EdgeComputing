import json


class ParseJson(object):

    def parser(self):

        # load Urban Observatory data in JSON format
        data = json.load(open('data.json'))
        readings = []

        # for each Air Quality sensor in the data set retrieve the relevant measurements
        for sensor in data:
            name = sensor["name"]
            geom = sensor["geom"]
            active = sensor["active"]
            dataSet = sensor["data"]
            baseHeight = sensor["base_height"]
            sensorHeight = sensor["sensor_height"]
            latest = sensor["latest"]
            source = sensor["source"]
            type = sensor["type"]

            # Iterates through the attributes of air quality measured (e.g Carbon Dioxide, Nitrogen Oxide)
            for airQualityAttribute in dataSet:

                # formats each reading in the data set as it's own message.
                for reading in dataSet[airQualityAttribute]["data"]:
                    readings.append({
                        "name": name,
                        "geom": geom,
                        "active": active,
                        "baseHeight": baseHeight,
                        "sensorHeight": sensorHeight,
                        "latest": latest,
                        "source": source,
                        "type": type,
                        "airQualityAttribute": airQualityAttribute,
                        "timestamp": reading,
                        "readingValue": dataSet[airQualityAttribute]["data"][reading]
                    })
        return readings
