import time
import random
from locust import FastHttpUser, task, constant_throughput, tag
from datetime import datetime, timedelta

class HotelReservationUser(FastHttpUser):
    wait_time = constant_throughput(1)
    
    # Constants from Lua script
    max_user_index = 500
    base_lat = 38.0235
    base_lon = -122.095

    def on_start(self):
        current_time_millis = int(time.time() * 1000)
        random.seed(current_time_millis)
        random.random(); random.random(); random.random()

    def get_user(self):
        user_id = random.randint(0, self.max_user_index)
        username = f"user_{user_id}"
        password = str(user_id) * 10
        return username, password

    def get_dates(self):
        in_date = random.randint(9, 23)
        out_date = random.randint(in_date + 1, 24)
        
        in_date_str = f"2015-04-{in_date:02d}"
        out_date_str = f"2015-04-{out_date:02d}"
        
        return in_date_str, out_date_str

    def get_location(self):
        lat = self.base_lat + (random.randint(0, 481) - 240.5) / 1000.0
        lon = self.base_lon + (random.randint(0, 325) - 157.0) / 1000.0
        return lat, lon

    @tag('search', 'mixed')
    @task(60)
    def search_hotel(self):
        in_date, out_date = self.get_dates()
        lat, lon = self.get_location()

        params = {
            "inDate": in_date,
            "outDate": out_date,
            "lat": lat,
            "lon": lon
        }

        self.client.get("/hotels", params=params, name="search-hotels")

    @tag('recommend', 'mixed')
    @task(38)
    def recommend(self):
        lat, lon = self.get_location()
        req_param = random.choice(["dis", "rate", "price"])

        params = {
            "require": req_param,
            "lat": lat,
            "lon": lon
        }

        self.client.get("/recommendations", params=params, name="get-recommendations")

    @tag('login', 'mixed')
    @task(1)
    def user_login(self):
        username, password = self.get_user()

        params = {
            "username": username,
            "password": password
        }

        self.client.post("/user", params=params, name="user-login")

    @tag('reserve', 'mixed')
    @task(1)
    def reserve(self):
        in_date, out_date = self.get_dates()
        lat, lon = self.get_location()
        username, password = self.get_user()
        hotel_id = str(random.randint(1, 80))

        params = {
            "inDate": in_date,
            "outDate": out_date,
            "lat": lat,
            "lon": lon,
            "hotelId": hotel_id,
            "customerName": username,
            "username": username,
            "password": password,
            "number": "1"
        }

        self.client.post("/reservation", params=params, name="make-reservation")
