from faxe import Faxe, Point, Batch


def test_point_value():
    p = Point.new()
    Point.value(p, 'value', 1)
    assert Point.value(p, 'value') == 1
    Point.value(p, 'messageType', 'guckguck')
    assert Point.value(p, 'messageType') == 'guckguck'
    Point.value(p, 'data.value', 2)
    assert Point.value(p, 'data.value') == 2
    Point.value(p, 'data.valuelist[0]', 3)
    assert Point.value(p, 'data.valuelist[0]') == 3
    Point.value(p, 'data.valuelist[1].one', 4)
    assert Point.value(p, 'data.valuelist[1].one') == 4
    assert Point.value(p, 'data.valuelist') == [3, {'one': 4}]
    assert Point.value(p, 'data.valuelist[2].one') is None


def test_point_values():
    fields = dict()
    fields['f1'] = {'deep': {'deeper': 49}}
    fields['f2'] = {'deep2': {'deeper2': 50}}
    p = Point.new()
    Point.fields(p, fields)
    assert Point.fields(p) == fields
    assert Point.values(p, ['f1.deep', 'f2.deep2.deeper2']) == [{'deeper': 49}, 50]
    assert Point.values(p, ['f1.deep', 'f2.deep2.deeper2', 'no.field']) == [{'deeper': 49}, 50, None]

    assert Point.fields(Point.new()) == {}


def test_batch_value():
    # pass
    p = Point.new(2)
    Point.value(p, 'data.valuelist[0].one', 4)
    b = Batch.new()
    Batch.points(b, [p])
    assert Batch.value(b, 'data.valuelist[0].one') == [4]
    assert Batch.points(b) == [p]
    p1 = Point.new(1)
    Point.value(p1, 'data.valuelist[0].one', 6.00023)
    Batch.add(b, p1)
    assert Batch.value(b, 'data.valuelist[0].one') == [6.00023, 4]
    assert Batch.value(b, 'data.valuelist') == [[{'one': 6.00023}], [{'one': 4}]]

    Batch.value(b, 'added.value', {'int': 55})
    assert Batch.value(b, 'added.value.int') == [55, 55]


def test_batch_values():
    # pass
    p = Point.new(2)
    Point.value(p, 'data.valuelist[0].one', 4)
    p2 = Point.new(4)
    Point.value(p, 'data.valuelist[0].one', '4')
    b = Batch.new()
    Batch.points(b, [p])
    assert Batch.value(b, 'data.valuelist[0].one') == [4]
    assert Batch.points(b) == [p]
    p1 = Point.new(1)
    Point.value(p1, 'data.valuelist[0].one', 6.00023)
    Batch.add(b, p1)
    Batch.add(b, p2)

    assert Batch.values(b, ['data.valuelist[0].one']) == [6.00023, 4]
    assert Batch.value(b, 'data.valuelist') == [[{'one': 6.00023}], [{'one': 4}]]

    Batch.value(b, 'added.value', {'int': 55})
    assert Batch.value(b, 'added.value.int') == [55, 55]


def test_point_ts():
    p = Point.new(3234232)
    assert Point.ts(p) == 3234232
    now = Faxe.now()
    Point.ts(p, now)
    assert Point.ts(p) == now

    p1 = Point.new()
    assert Point.ts(p1) is None


if __name__ == "__main__":
    test_point_value()
    test_point_values()
    test_point_ts()
    test_batch_value()
    print("Everything passed")
