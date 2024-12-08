import time
import random
from locust import FastHttpUser, task, tag, events
import numpy as np

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--wait-distribution", "-w",
        type=str,
        env_var="LOCUST_WAIT_DISTRIBUTION",
        choices=["fixed", "exp", "zipf"],
        default="fixed",
        help="Wait time distribution (fixed, exp, or zipf)"
    )
    parser.add_argument(
        "--throughput-per-user", "-tu",
        type=float,
        env_var="LOCUST_THROUGHPUT_PER_USER",
        default=1.0,  # 1 request per second
        help="Throughput per user in requests per second (only for fixed distribution)"
    )
    parser.add_argument(
        "--zipf-alpha", "-za",
        type=float,
        env_var="LOCUST_ZIPF_ALPHA",
        default=3.0,
        help="Zipf distribution shape parameter"
    )

class SocialNetworkUser(FastHttpUser):
    # Load environment variables or set default
    max_user_index = 961

    # Add class variables for character sets
    charset = list('qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890')
    decset = list('1234567890')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Get configuration from command line args
        self.wait_distribution = self.environment.parsed_options.wait_distribution
        self.throughput_per_user = self.environment.parsed_options.throughput_per_user
        self.zipf_alpha = self.environment.parsed_options.zipf_alpha
        self._setup_wait_time()
        
    def _setup_wait_time(self):
        """Configure the wait time function based on distribution type"""
        if self.wait_distribution == "fixed":
            self.wait_time = lambda : 1.0/self.throughput_per_user
        elif self.wait_distribution == "exp":
            self.wait_time = lambda : random.expovariate(self.throughput_per_user)
        elif self.wait_distribution == "zipf":
            self.wait_time = lambda : np.random.zipf(self.zipf_alpha)/self.throughput_per_user
        else:
            raise ValueError(f"Unknown distribution type: {self.wait_distribution}")

    def on_start(self):
        # Seed random with the current time (milliseconds)
        current_time_millis = int(time.time() * 1000)
        random.seed(current_time_millis)
        random.random(); random.random(); random.random()  # Warm up the RNG

    def string_random(self, length):
        return ''.join(random.choice(self.charset) for _ in range(length))

    def dec_random(self, length):
        return ''.join(random.choice(self.decset) for _ in range(length))
    
    @tag('read_home_timeline', 'mixed')
    @task(60)
    def read_home_timeline(self):
        # Generate random user_id, start, stop, and milliseconds_int
        user_id = str(random.randint(0, self.max_user_index - 1))
        start = str(random.randint(0, 30))
        stop = str(int(start) + random.randint(1, 20))
        milliseconds_int = int(time.time() * 1000)

        # Construct query parameters
        params = {
            "user_id": user_id,
            "start": start,
            "stop": stop,
            "req_id": milliseconds_int
        }

        # Send GET request to /wrk2-api/home-timeline/read
        self.client.get("/wrk2-api/home-timeline/read", params=params, 
                        headers={"Content-Type": "application/x-www-form-urlencoded"}, 
                        name="read-home-timeline")

    @tag('read_user_timeline', 'mixed')
    @task(30)
    def read_user_timeline(self):
        user_id = str(random.randint(0, self.max_user_index - 1))
        start = str(random.randint(0, 100))
        stop = str(int(start) + 10)

        params = {
            "user_id": user_id,
            "start": start,
            "stop": stop
        }

        self.client.get("/wrk2-api/user-timeline/read", params=params, 
                       headers={"Content-Type": "application/x-www-form-urlencoded"},
                       name="read-user-timeline")

    
    @tag('compose_post', 'mixed')
    @task(10)
    def compose_post(self):
        user_index = random.randint(0, self.max_user_index - 1)
        username = f"username_{user_index}"
        user_id = str(user_index)
        text = self.string_random(256)
        
        # Add mentions
        num_user_mentions = random.randint(0, 5)
        for _ in range(num_user_mentions):
            while True:
                mention_id = random.randint(0, self.max_user_index - 1)
                if mention_id != user_index:
                    break
            text += f" @username_{mention_id}"

        # Add URLs
        num_urls = random.randint(0, 5)
        for _ in range(num_urls):
            text += f" http://{self.string_random(64)}"

        # Add media
        num_media = random.randint(0, 4)
        media_ids = []
        media_types = []
        for _ in range(num_media):
            media_ids.append(self.dec_random(18))
            media_types.append("png")

        data = {
            "username": username,
            "user_id": user_id,
            "text": text,
            "media_ids": str(media_ids),
            "media_types": str(media_types),
            "post_type": "0"
        }

        self.client.post("/wrk2-api/post/compose",
                        data=data,
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        name="compose-post")
