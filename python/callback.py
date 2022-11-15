from faxe import Faxe


class Callback(Faxe):
    """
    simple noop faxe python node callback
    """

    def handle_point(self, point_data):
        self.emit(point_data)

    def handle_batch(self, batch_data):
        self.emit(batch_data)
