from faxe import Faxe


class Callback(Faxe):
    """
    simple noop faxe python node callback
    """

    @staticmethod
    def options():
        opts = []
        return opts

    def init(self, args):
        print("my args: ", args)

    def handle_point(self, point_data):
        print("point ", point_data)
        return self.emit(point_data)

    def handle_batch(self, batch_data):
        print("batch ", batch_data)
        return self.emit(batch_data)
