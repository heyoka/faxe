from datetime import datetime
import lmapi


def get_time():
    return datetime.utcnow()


def get_ms():
    return lmapi.LMApi.current_ms()
