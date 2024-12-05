import time
import random
from locust import FastHttpUser, task, constant_throughput, tag

class SocialNetworkUser(FastHttpUser):
    # wait_time = between(1, 2)  # Define wait time between requests
    wait_time = constant_throughput(1)

    # Load environment variables or set default
    max_user_index = 961

    # Add class variables for character sets
    charset = list('qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890')
    decset = list('1234567890')

    def on_start(self):
        # Seed random with the current time (milliseconds)
        current_time_millis = int(time.time() * 1000)
        random.seed(current_time_millis)
        random.random(); random.random(); random.random()  # Warm up the RNG

    def string_random(self, length):
        return ''.join(random.choice(self.charset) for _ in range(length))

    def dec_random(self, length):
        return ''.join(random.choice(self.decset) for _ in range(length))
    
    @tag('rht', 'mixed')
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

    @tag('rut', 'mixed')
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

    
    @tag('cp', 'mixed')
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
