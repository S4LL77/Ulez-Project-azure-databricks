import json
import re
import uuid

import requests
from dotenv import load_dotenv

# Load Snowflake/API env...
load_dotenv()


def fetch_autotrader_listings(
    make="BMW", postcode="SW1A1AA", pages=1, fuel_type_filter=None
):
    """
    Fetches live listings from AutoTrader UK (v2.2).
    Supports explicit fuel type filtering via the API.
    """
    url = (
        "https://www.autotrader.co.uk/at-gateway?opname=SearchResultsListingsGridQuery"
    )

    headers = {
        "Content-Type": "application/json",
        "x-sauron-app-name": "sauron-search-results-app",
        "x-sauron-app-version": "3c3ad1557c",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    search_id = str(uuid.uuid4())

    # Base filters
    api_filters = [
        {"filter": "make", "selected": [make]},
        {"filter": "postcode", "selected": [postcode]},
        {"filter": "price_search_type", "selected": ["total"]},
    ]

    # Add fuel type filter if requested
    if fuel_type_filter:
        api_filters.append({"filter": "fuel_type", "selected": [fuel_type_filter]})

    payload = [
        {
            "operationName": "SearchResultsListingsGridQuery",
            "variables": {
                "filters": api_filters,
                "channel": "cars",
                "page": pages,
                "sortBy": "relevance",
                "listingType": None,
                "searchId": search_id,
                "featureFlags": ["USE_NEW_LEASE_CARD"],
            },
            "query": """
            query SearchResultsListingsGridQuery($filters: [FilterInput!]!, $channel: Channel!, $page: Int, $sortBy: SearchResultsSort, $listingType: [ListingType!], $searchId: String!, $featureFlags: [FeatureFlag]) {
              searchResults(
                input: {facets: [], filters: $filters, channel: $channel, page: $page, sortBy: $sortBy, listingType: $listingType, searchId: $searchId, featureFlags: $featureFlags}
              ) {
                listings {
                  ... on SearchListing {
                    advertId
                    title
                    subTitle
                    attentionGrabber
                    price
                    badges {
                      type
                      displayText
                    }
                    trackingContext {
                      advertContext {
                        make
                        model
                        year
                        price
                      }
                    }
                  }
                }
              }
            }
        """,
        }
    ]

    print(f"Fetching live {make} {fuel_type_filter or ''} listings (v2.2)...")
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        raw_data = response.json()
        raw_listings = (
            raw_data[0].get("data", {}).get("searchResults", {}).get("listings", [])
        )

        listings = []
        for l in raw_listings:
            if "trackingContext" in l:
                ctx = l["trackingContext"].get("advertContext", {})
                badges = l.get("badges", [])
                sub_title = l.get("subTitle") or ""
                grabber = l.get("attentionGrabber") or ""

                # Extract mileage from badges
                mileage = 0
                for badge in badges:
                    if badge.get("type") == "MILEAGE":
                        m_str = (
                            badge.get("displayText", "0").replace(",", "").split(" ")[0]
                        )
                        try:
                            mileage = int(m_str)
                        except:
                            pass

                # Fuel Type from Filter or Heuristic
                current_fuel = fuel_type_filter
                if not current_fuel:
                    if "Petrol" in grabber or "Petrol" in sub_title:
                        current_fuel = "Petrol"
                    elif "Diesel" in grabber or "Diesel" in sub_title:
                        current_fuel = "Diesel"
                    elif "Hybrid" in grabber or "Hybrid" in sub_title:
                        current_fuel = "Hybrid"
                    elif "Electric" in grabber or "Electric" in sub_title:
                        current_fuel = "Electric"
                    else:
                        current_fuel = "Unknown"

                # Transmission heuristic
                transmission = "Manual"
                if "Auto" in sub_title or "Automatic" in sub_title or "Auto" in grabber:
                    transmission = "Automatic"

                # Engine Size heuristic
                engine_size = 0.0
                engine_match = re.search(r"(\d\.\d)", sub_title)
                if engine_match:
                    engine_size = float(engine_match.group(1))

                listings.append({
                        "id": l.get("advertId"),
                        "title": l.get("title"),
                        "brand": ctx.get("make"),
                        "model": ctx.get("model"),
                        "price": ctx.get("price"),
                        "year": ctx.get("year"),
                        "mileage": mileage,
                        "fuelType": current_fuel,
                        "engineSize": engine_size,
                        "transmission": transmission,
                    }
                )

        print(f"OK: Fetched {len(listings)} listings.")
        return listings
    else:
        print(f"ERROR: {response.status_code}")
        return []


if __name__ == "__main__":
    # Test standalone with filter
    test_data = fetch_autotrader_listings(make="BMW", fuel_type_filter="Diesel")
    if test_data:
        print("\n--- SAMPLE DIESEL LISTING ---")
        print(json.dumps(test_data[0], indent=2))
