import time
import random
from locust import FastHttpUser, task, constant_throughput, constant, tag, events
from datetime import datetime, timedelta
import numpy as np

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--wait-distribution", "-w",
        type=str,
        env_var="LOCUST_WAIT_DISTRIBUTION",
        choices=["fixed", "constput", "exp", "zipf"],
        default="fixed",
        help="Wait time distribution (fixed, constant throughput (constput), exp, or zipf)"
    )
    parser.add_argument(
        "--throughput-per-user", "-tu",
        type=float,
        env_var="LOCUST_THROUGHPUT_PER_USER",
        default=1.0,
        help="Throughput per user in requests per second (only for fixed distribution)"
    )
    parser.add_argument(
        "--zipf-alpha", "-za",
        type=float,
        env_var="LOCUST_ZIPF_ALPHA",
        default=3.0,
        help="Zipf distribution shape parameter"
    )

class HotelReservationUser(FastHttpUser):

    max_user_index = 500
    base_lat = 38.0235
    base_lon = -122.095

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wait_distribution = self.environment.parsed_options.wait_distribution
        self.throughput_per_user = self.environment.parsed_options.throughput_per_user
        self.zipf_alpha = self.environment.parsed_options.zipf_alpha
        self._setup_wait_time()

    def _setup_wait_time(self):
        """Configure the wait time function based on distribution type"""
        if self.wait_distribution == "constput":
            self.wait_time = constant_throughput(self.throughput_per_user)
        elif self.wait_distribution == "fixed":
            self.wait_time = lambda: 1.0 / self.throughput_per_user
        elif self.wait_distribution == "exp":
            self.wait_time = lambda: random.expovariate(self.throughput_per_user)
        elif self.wait_distribution == "zipf":
            self.wait_time = lambda: np.random.zipf(self.zipf_alpha) / self.throughput_per_user
        else:
            raise ValueError(f"Unknown distribution type: {self.wait_distribution}")

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
