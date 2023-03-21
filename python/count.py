from faxe import Faxe, Point, Batch
from datetime import datetime


class Count(Faxe):

    def state_mode(self):
        # return Faxe.STATE_MODE_MANUAL
        # return Faxe.STATE_MODE_HANDLE
        return Faxe.STATE_MODE_EMIT

    def format_state(self):
        return {'count': self.count, 'since': self.since}

    def init(self, _args):
        self.log(f"state for {__name__} : {self.get_state()}")
        self.count = self.get_state_value('count', 0)
        self.since = self.get_state_value('since', Faxe.now())

    def handle_point(self, _point_data):
        self.count += 1
        self.emit_count()

    def handle_batch(self, batch_data):
        self.count += len(Batch.points(batch_data))
        self.emit_count()

    def emit_count(self):
        out = Point.new()
        Point.value(out, 'point_count', self.count)
        Point.value(out, 'count_since', datetime.fromtimestamp(self.since/1000.0).strftime("%m/%d/%Y, %H:%M:%S"))
        Point.value(out, 'seconds_ago', (Faxe.now() - self.since)//1000)
        self.emit(out)
        # self.persist_state()

