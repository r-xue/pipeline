import pytest

from .executeppr import _sanitize_for_ms

test_params = [('uid__A002_target.msXd3e89f_Xc53e', 'uid__A002_target.msXd3e89f_Xc53e'),
               ('uid__A002_target.msXd3e89f_Xc53e.ms', 'uid__A002_target.msXd3e89f_Xc53e'),
               ('uid__A002_target.msXd3e89f_Xc53e_target.ms', 'uid__A002_target.msXd3e89f_Xc53e'),
               ('uid__A002_target.msXd3e89f_Xc53e_target.ms_target.ms', 'uid__A002_target.msXd3e89f_Xc53e'),
               ('uid__A002_target.msXd3e89f_Xc53e_target.ms_target.ms.ms.ms.ms', 'uid__A002_target.msXd3e89f_Xc53e')]

@pytest.mark.parametrize("visname, expected", test_params)
def test_sanitize_ms(visname, expected):
    """Test _sanitize_for_ms() from executeppr
    """
    assert _sanitize_for_ms(visname) == expected