from forge.cpd3.variant import serialize, deserialize, Overlay, Matrix, Keyframe, MetadataReal, MetadataHash


def test_empty():
    data = serialize(None)
    assert data is not None
    data = deserialize(data)
    assert data is None


def test_real():
    data = serialize(1.25)
    data = deserialize(data)
    assert data == 1.25


def test_integer():
    data = serialize(42)
    bad = serialize(42.0)
    assert data != bad
    data = deserialize(data)
    assert data == 42


def test_boolean():
    data = serialize(True)
    bad = serialize(1)
    assert data != bad
    data = deserialize(data)
    assert data == True

    data = serialize(False)
    bad = serialize(0)
    assert data != bad
    data = deserialize(data)
    assert data == False


def test_bytes():
    data = serialize(b'123')
    bad = serialize('123')
    assert data != bad
    data = deserialize(data)
    assert data == b'123'


def test_string():
    data = serialize('123')
    bad = serialize(b'123')
    assert data != bad
    data = deserialize(data)
    assert data == '123'


def test_flags():
    data = serialize({'Flag1', 'Flag2'})
    data = deserialize(data)
    assert data == {'Flag1', 'Flag2'}


def test_overlay():
    data = serialize(Overlay('/Stuff'))
    bad = serialize('/Stuff')
    assert data != bad
    data = deserialize(data)
    assert isinstance(data, Overlay)
    assert data == Overlay('/Stuff')


def test_array():
    data = serialize(['First', 2.25, 3, None])
    data = deserialize(data)
    assert data == ['First', 2.25, 3, None]


def test_matrix():
    m = Matrix([1, 2, 3, 4, 5, 6, 7, 8])
    m.shape = [2, 4]
    data = serialize(m)
    data = deserialize(data)
    assert data == m
    assert data.shape == [2, 4]


def test_hash():
    data = serialize({'First': 2.0, 'Second': [1, 2]})
    data = deserialize(data)
    assert data == {'First': 2.0, 'Second': [1, 2]}


def test_keyframe():
    k = Keyframe({1.0: 'First', 2.25: 'Second'})
    data = serialize(k)
    data = deserialize(data)
    assert data == k
    assert data[1.0] == 'First'


def test_metadatareal():
    m = MetadataReal({'First': 'Value', 'Second': 123})
    data = serialize(m)
    data = deserialize(data)
    assert data == m
    assert isinstance(data, MetadataReal)


def test_metadatahash():
    m = MetadataHash({'First': 'Value', 'Second': 123})
    m.children['Key'] = {'F1', 'F2'}
    data = serialize(m)
    data = deserialize(data)
    assert data == m
    assert isinstance(data, MetadataHash)
    assert data.children == {'Key': {'F1', 'F2'}}
