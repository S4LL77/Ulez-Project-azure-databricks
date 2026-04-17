import sys
from pathlib import Path

# Add the ingestion directory to the Python path for test imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "01_ingestion"))

from autotrader_collector import fetch_autotrader_listings


def test_fetch_autotrader_listings_success(mocker):
    # Mock response from requests.post
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {
            "data": {
                "searchResults": {
                    "listings": [
                        {
                            "advertId": "12345",
                            "title": "BMW 3 Series",
                            "subTitle": "2.0 320d M Sport Auto 4dr",
                            "attentionGrabber": "Great Diesel Car",
                            "price": "£15,000",
                            "badges": [
                                {"type": "MILEAGE", "displayText": "50,000 miles"}
                            ],
                            "trackingContext": {
                                "advertContext": {
                                    "make": "BMW",
                                    "model": "3 Series",
                                    "year": 2018,
                                    "price": 15000,
                                }
                            },
                        }
                    ]
                }
            }
        }
    ]

    mocker.patch("requests.post", return_value=mock_response)

    listings = fetch_autotrader_listings(make="BMW")

    assert len(listings) == 1
    assert listings[0]["id"] == "12345"
    assert listings[0]["price"] == 15000
    assert listings[0]["fuelType"] == "Diesel"  # Extracted from grabber/subtitle
    assert listings[0]["transmission"] == "Automatic"  # Extracted from subtitle
    assert listings[0]["mileage"] == 50000


def test_fetch_autotrader_listings_failure(mocker):
    mock_response = mocker.Mock()
    mock_response.status_code = 500
    mocker.patch("requests.post", return_value=mock_response)

    listings = fetch_autotrader_listings(make="BMW")
    assert listings == []
