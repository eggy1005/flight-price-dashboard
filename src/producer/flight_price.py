import requests


def run():
    url = "https://booking-com15.p.rapidapi.com/api/v1/flights/searchFlights"

    querystring = {"fromId": "LHR.AIRPORT", "toId": "HKG.AIRPORT", "departDate": "2024-05-07", "pageNo": "1",
                   "adults": "1", "currency_code": "USD"}

    headers = {
        "X-RapidAPI-Key": "7dc28ae600msh79f3a98b8c6e20ep12b2a3jsncf8925468838",
        "X-RapidAPI-Host": "booking-com15.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    print(response.json())


if __name__ == '__main__':
    run()
