from locust import HttpUser, task, between, constant_pacing
import random


class MicroserviceUser(HttpUser):
    """
    Змішаний профіль навантаження: легкі та важкі запити у співвідношенні 3:7.
    Моделює реалістичний розподіл трафіку мікросервісу.
    """
    wait_time = between(0.5, 2.0)

    @task(3)
    def root_request(self):
        self.client.get("/", name="GET /")

    @task(7)
    def heavy_request(self):
        delay = round(random.uniform(0.8, 2.0), 1)
        self.client.get(f"/heavy-task?delay={delay}", name="GET /heavy-task")


class SpikeUser(HttpUser):
    """
    Профіль пікового навантаження для тестування реакції предиктивного контролера.
    Фіксований темп 10 запитів/с на користувача.
    """
    wait_time = constant_pacing(0.1)

    @task
    def spike_request(self):
        delay = round(random.uniform(1.0, 1.5), 1)
        self.client.get(f"/heavy-task?delay={delay}", name="GET /heavy-task [spike]")
