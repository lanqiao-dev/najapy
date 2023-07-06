import pytest

from najapy.common.async_base import Utils


class TestPhoneFormat:
    def test_valid_phone_with_area_code(self):
        assert Utils.validate_phone_simple_format('+8613346198808') == '+8613346198808'
        assert Utils.validate_phone_simple_format('8613346198808') == '8613346198808'

    def test_valid_phone_without_area_code(self):
        assert Utils.validate_phone_simple_format('13346198808') == '13346198808'

    def test_valid_phone_without_area_code_explicitly(self):
        assert Utils.validate_phone_simple_format('+8613346198808', True) == '13346198808'
        assert Utils.validate_phone_simple_format('8613346198808', True) == '13346198808'

    def test_invalid_phone(self):
        assert Utils.validate_phone_simple_format('+8513346198808') is None
        assert Utils.validate_phone_simple_format('861a346198808') is None
        assert Utils.validate_phone_simple_format('133461988') is None

    def test_empty_phone(self):
        assert Utils.validate_phone_simple_format('') is None


@pytest.mark.parametrize("id_number, expected", [
        ("130130198610130", "130130******130"),
        ("130130198610130007", "130130********0007"),
        ("abcd", "abcd")
    ])
def test_mask_id_number(id_number, expected):
    assert Utils.mask_id_number(id_number) == expected
