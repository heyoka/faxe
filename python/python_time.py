from faxe import Faxe, Point, Batch


class Python_time(Faxe):

    # def handle_batch(self, b):
    #     self.log('handle_batch is implemented ;)')

    def handle_point(self, point_data):
        # add the field python.time with the current timestamp
        now = Faxe.now()
        Point.value(point_data, 'python.time', now)
        batch = Batch.new(now)
        batch['points'] = [point_data]
        self.emit(batch)

