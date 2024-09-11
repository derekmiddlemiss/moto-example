import pytest
from botocore.response import StreamingBody
from src.get_count_from_object import get_count_from_object


@pytest.fixture
def s3_obj_factory(mocker):
    def _create_s3_obj_response(body_string: str) -> mocker.MagicMock:
        mock_s3_body = mocker.MagicMock(spec=StreamingBody)
        mock_s3_body.read.return_value = bytes(body_string, 'utf-8')
        spec_dict = {'Body': mock_s3_body}

        mock_s3_obj = mocker.MagicMock()
        mock_s3_obj.__getitem__ = mocker.MagicMock()
        mock_s3_obj.__getitem__.side_effect = spec_dict.__getitem__
        return mock_s3_obj

    return _create_s3_obj_response


def test_get_count_from_object__one_count(s3_obj_factory):
    one_count_obj = s3_obj_factory('Count:  15')
    assert get_count_from_object(one_count_obj) == ['15']


