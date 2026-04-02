import requests


def test_health_check():
    url = (
        "https://b5hxtt8xp6.execute-api.ap-southeast-2.amazonaws.com"
        "/dev/collect/health"
    )
    r = requests.get(url)
    assert r.status_code == 200
    assert r.json()['status'] == "healthy"
