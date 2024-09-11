import re
from mypy_boto3_s3.type_defs import GetObjectOutputTypeDef
from .utilities.single_or_empty import check_single_or_empty


def get_count_from_object(obj: GetObjectOutputTypeDef) -> list[str]:
    file_content = obj['Body'].read().decode('utf-8')
    return check_single_or_empty(
        re.findall(
            pattern=r'count\s*:\s*(\d+)',
            string=file_content,
            flags=re.IGNORECASE
        )
    )


