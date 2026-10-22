"""Configuration shared by every service.

Everything comes from environment variables so the same image runs unchanged under
plain Python, Docker Compose and Kubernetes -- only the *addresses* change, never the code.
"""
import os
import socket

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://ticketrush:ticketrush@localhost:5432/ticketrush")
CATALOG_URL = os.getenv("CATALOG_URL", "http://localhost:8001")
BOOKING_URL = os.getenv("BOOKING_URL", "http://localhost:8002")

# In Docker/Kubernetes the hostname is the container / pod name, which makes every
# replica uniquely identifiable on the dashboard.
INSTANCE_ID = os.getenv("INSTANCE_ID") or socket.gethostname()

DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))

# Booking rules
HOLD_SECONDS = int(os.getenv("HOLD_SECONDS", "120"))      # unpaid reservations expire after this
MAX_TICKETS_PER_USER = int(os.getenv("MAX_TICKETS_PER_USER", "4"))
REAPER_INTERVAL = float(os.getenv("REAPER_INTERVAL", "5"))

# Payment simulation
PAYMENT_FAIL_RATE = float(os.getenv("PAYMENT_FAIL_RATE", "0.10"))
PAYMENT_MIN_MS = int(os.getenv("PAYMENT_MIN_MS", "200"))
PAYMENT_MAX_MS = int(os.getenv("PAYMENT_MAX_MS", "600"))
PAYMENT_CPU_ROUNDS = int(os.getenv("PAYMENT_CPU_ROUNDS", "20000"))  # simulated fraud scoring
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "20"))
RECLAIM_IDLE_MS = int(os.getenv("RECLAIM_IDLE_MS", "30000"))

# Redis names
ORDERS_STREAM = "orders"
PAYMENT_GROUP = "payments"
EVENTS_CHANNEL = "ticketrush:events"
HEARTBEAT_TTL = 10


def inventory_key(event_id: int) -> str:
    return f"inventory:{event_id}"


def user_key(event_id: int, user_id: str) -> str:
    return f"user:{event_id}:{user_id}"


def released_key(order_id: str) -> str:
    return f"released:{order_id}"
