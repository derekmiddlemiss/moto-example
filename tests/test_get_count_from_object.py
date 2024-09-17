from typing import Callable

import pytest
from botocore.response import StreamingBody
from src.get_count_from_object import get_count_from_object
from unittest.mock import MagicMock


@pytest.fixture
def s3_obj_response_factory(mocker) -> Callable[[str], MagicMock]:
    def _create_s3_obj_response(body_string: str) -> MagicMock:
        mock_s3_body = mocker.MagicMock(spec=StreamingBody)
        mock_s3_body.read.return_value = bytes(body_string, 'utf-8')
        spec_dict = {'Body': mock_s3_body}

        mock_s3_obj = mocker.MagicMock()
        mock_s3_obj.__getitem__ = mocker.MagicMock()
        mock_s3_obj.__getitem__.side_effect = spec_dict.__getitem__
        return mock_s3_obj

    return _create_s3_obj_response


def test_get_count_from_object__one_count(s3_obj_response_factory):
    one_count_obj = s3_obj_response_factory('count: 10')
    assert get_count_from_object(one_count_obj) == ['10']


def test_get_count_from_object__one_count_capital_spaces(s3_obj_response_factory):
    one_count_obj = s3_obj_response_factory('COUNT  :  15')
    assert get_count_from_object(one_count_obj) == ['15']


def test_get_count_from_object__one_count_embedded_in_text(s3_obj_response_factory):
    one_count_obj = s3_obj_response_factory('Test text COUNT  :  15 blah')
    assert get_count_from_object(one_count_obj) == ['15']


def test_get_count_from_object__no_count(s3_obj_response_factory):
    one_count_obj = s3_obj_response_factory('Test text  :  15 blah')
    assert get_count_from_object(one_count_obj) == []


def test_get_count_from_object__empty_string(s3_obj_response_factory):
    one_count_obj = s3_obj_response_factory('')
    assert get_count_from_object(one_count_obj) == []


def test_get_count_from_object__two_counts_raises(s3_obj_response_factory):
    two_count_obj = s3_obj_response_factory('Yappity yap count:  10 more waffle COUNT: 12 piffle paffle')
    with pytest.raises(ValueError, match='Length of input list must be zero or one.'):
        get_count_from_object(two_count_obj)


