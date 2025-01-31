### producer_main.py
import json
import time
import requests
from confluent_kafka import Producer

# API Client Base Class
class APIClient:
    def fetch_data(self, city: str):
        raise NotImplementedError("Subclasses must implement the fetch_data method.")

# AirQualityAPIClient: Fetches data from AQICN
class AirQualityAPIClient(APIClient):
    def __init__(self, api_url: str, api_token: str):
        self.api_url = api_url
        self.api_token = api_token

    def fetch_data(self, city: str):
        url = f"{self.api_url}{city}/"
        params = {"token": self.api_token}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

# WeatherAPIClient: Fetches data from OpenWeatherMap
class WeatherAPIClient(APIClient):
    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url
        self.api_key = api_key

    def fetch_data(self, city: str):
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "metric"
        }
        response = requests.get(self.api_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

# KafkaProducerService: Handles Kafka message production
class KafkaProducerService:
    def __init__(self, bootstrap_servers: str):
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})

    def produce_message(self, topic: str, key: str, value: dict):
        self.producer.produce(
            topic,
            key=key,
            value=json.dumps(value),
            callback=self._delivery_report
        )
        self.producer.flush()

    def _delivery_report(self, err, msg):
        if err:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# RealTimePipeline: Orchestrates the producer pipeline
class RealTimeProducerPipeline:
    def __init__(self, city: str, producer_service: KafkaProducerService, air_quality_client: AirQualityAPIClient, weather_client: WeatherAPIClient):
        self.city = city
        self.producer_service = producer_service
        self.air_quality_client = air_quality_client
        self.weather_client = weather_client

    def run(self, air_quality_topic: str, weather_topic: str, interval: int = 60):
        while True:
            try:
                # Fetch data from APIs
                aqi_data = self.air_quality_client.fetch_data(self.city)
                weather_data = self.weather_client.fetch_data(self.city)

                # Produce data to Kafka
                self.producer_service.produce_message(air_quality_topic, self.city, aqi_data)
                self.producer_service.produce_message(weather_topic, self.city, weather_data)

                print(f"Data for {self.city} pushed to Kafka topics.")

                # Sleep for the specified interval
                time.sleep(interval)
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(10)

# Main function for the producer pipeline
if __name__ == "__main__":
    # Configuration for producer
    CITY = "Los Angeles"
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    AIR_QUALITY_TOPIC = "air_quality_data"
    WEATHER_TOPIC = "weather_data"
    AQICN_API_URL = "https://api.waqi.info/feed/"
    AQICN_API_TOKEN = "your_aqicn_api_token"
    OPENWEATHERMAP_API_URL = "https://api.openweathermap.org/data/2.5/weather"
    OPENWEATHERMAP_API_KEY = "1436ff089b74fbe028f84ae9d8631c90"

    # Initialize producer components
    producer_service = KafkaProducerService(KAFKA_BOOTSTRAP_SERVERS)
    air_quality_client = AirQualityAPIClient(AQICN_API_URL, AQICN_API_TOKEN)
    weather_client = WeatherAPIClient(OPENWEATHERMAP_API_URL, OPENWEATHERMAP_API_KEY)

    # Start the producer pipeline
    producer_pipeline = RealTimeProducerPipeline(CITY, producer_service, air_quality_client, weather_client)
    producer_pipeline.run(AIR_QUALITY_TOPIC, WEATHER_TOPIC)

# OPENWEATHERMAP_API_URL = "https://api.openweathermap.org/data/2.5/weather"
# OPENWEATHERMAP_API_KEY = "1436ff089b74fbe028f84ae9d8631c90"