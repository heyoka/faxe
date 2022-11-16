from faxe import Faxe, Point


class Python_time(Faxe):

    # def handle_batch(self, b):
    #     self.log('handle_batch is implemented ;)')

    def handle_point(self, point_data):
        # add the field python.time with the current timestamp
        Point.value(point_data, 'python.time', Faxe.now())
        self.emit(point_data)

