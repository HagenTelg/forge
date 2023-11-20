import pytest
import typing
import numpy as np
from forge.data.merge.timealign import incoming_before, peer_output_time


def test_before():
    def permuted(destination, incoming, expected):
        destination = np.array(destination, dtype=np.int64)
        incoming = np.array(incoming, dtype=np.int64)
        assert incoming_before(destination, incoming, sort_destination=True, sort_incoming=True).tolist() == expected
        assert incoming_before(destination, incoming, sort_destination=True).tolist() == expected
        assert incoming_before(destination, incoming, sort_incoming=True).tolist() == expected
        assert incoming_before(destination, incoming).tolist() == expected

    assert incoming_before(
        np.array([300, 200, 100], dtype=np.int64),
        np.array([200, 300, 100], dtype=np.int64),
        sort_destination=True, sort_incoming=True,
    ).tolist() == [1, 0, 2]

    permuted([99], [100], [0])
    permuted([100], [100], [0])
    permuted([100], [101], [0])
    permuted([101], [100], [0])

    permuted([100, 200], [100], [0, 0])
    permuted([100, 200], [101], [0, 0])
    permuted([101, 200], [100], [0, 0])
    permuted([100, 200], [200], [0, 0])

    permuted([99], [100, 200], [0])
    permuted([100], [100, 200], [0])
    permuted([101], [100, 200], [0])
    permuted([200], [100, 200], [1])
    permuted([201], [100, 200], [1])

    permuted([100, 200], [100, 200], [0, 1])
    permuted([100, 200], [99, 200], [0, 1])
    permuted([100, 200], [101, 200], [0, 1])
    permuted([100, 200], [100, 199], [0, 1])
    permuted([100, 200], [100, 201], [0, 0])
    permuted([100, 200], [98, 99], [1, 1])
    permuted([200, 300], [100, 200], [1, 1])
    permuted([201, 300], [100, 200], [1, 1])

    permuted([100, 200, 300], [100, 200], [0, 1, 1])
    permuted([200, 300, 400], [100, 200], [1, 1, 1])
    permuted([100, 200, 300], [400, 500], [0, 0, 0])

    permuted([100, 200], [100, 200, 300], [0, 1])
    permuted([200, 300], [100, 200, 300], [1, 2])
    permuted([300, 400], [100, 200, 300], [2, 2])
    permuted([400, 500], [100, 200, 300], [2, 2])
    permuted([98, 99], [100, 200, 300], [0, 0])

    permuted([100, 200, 300], [100, 200, 300], [0, 1, 2])
    permuted([100, 199, 300], [100, 200, 300], [0, 0, 2])
    permuted([100, 201, 300], [100, 200, 300], [0, 1, 2])
    permuted([100, 200, 301], [100, 200, 300], [0, 1, 2])
    permuted([100, 200, 299], [100, 200, 300], [0, 1, 1])
    permuted([99, 200, 300], [100, 200, 300], [0, 1, 2])
    permuted([101, 200, 300], [100, 200, 300], [0, 1, 2])
    permuted([200, 300, 400], [100, 200, 300], [1, 2, 2])
    permuted([99, 100, 200], [100, 200, 300], [0, 0, 1])
    permuted([300, 400, 500], [100, 200, 300], [2, 2, 2])
    permuted([97, 98, 99], [100, 200, 300], [0, 0, 0])


def test_peer_output():
    assert peer_output_time(np.array([])).tolist() == []
    assert peer_output_time(
        np.array([100, 200, 300])
    ).tolist() == [100, 200, 300]
    assert peer_output_time(
        np.array([100, 200, 300]),
        np.array([100, 200, 300]),
    ).tolist() == [100, 200, 300]
    assert peer_output_time(
        np.array([100, 200, 300, 400]),
        np.array([100, 201, 300]),
    ).tolist() == [100, 200, 300, 400]

