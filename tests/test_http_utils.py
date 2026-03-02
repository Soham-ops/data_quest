from rearc_data_quest.http_utils import build_session

#BLS can block requests without proper User-Agent/contact info.
#This test makes sure your code keeps that setting correctly.

def test_build_session_sets_user_agent():
    session = build_session("Test Agent")
    assert session.headers["User-Agent"] == "Test Agent"

