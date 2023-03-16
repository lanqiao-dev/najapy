import os
import pytest

from najapy.common.async_base import Utils

pytestmark = pytest.mark.asyncio


class TestUtils:
    _utils = Utils()

    async def test_rsa_1(self):
        text = r"这是一个测试rsa数据"

        pub_key_file_name = os.path.join(
            os.path.split(os.path.abspath(__file__))[0],
            r'test_rsa_public_key.pem'
        )
        encryption_text = self._utils.rsa_encryption(pub_key_file_name, text)

        pri_key_file_name = os.path.join(
            os.path.split(os.path.abspath(__file__))[0],
            r'test_rsa_private_key.pem'
        )
        decryption_text = self._utils.rsa_decryption(pri_key_file_name, encryption_text)

        assert decryption_text == text

    async def test_rsa_2(self):
        text = r"这是一个测试rsa数据" * 1000

        pub_key_file_name = os.path.join(
            os.path.split(os.path.abspath(__file__))[0],
            r'test_rsa_public_key.pem'
        )
        encryption_text = self._utils.rsa_encryption(pub_key_file_name, text)

        pri_key_file_name = os.path.join(
            os.path.split(os.path.abspath(__file__))[0],
            r'test_rsa_private_key.pem'
        )
        decryption_text = self._utils.rsa_decryption(pri_key_file_name, encryption_text)

        assert decryption_text == text
