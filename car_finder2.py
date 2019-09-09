from bs4 import BeautifulSoup as bs
import requests
import pickle
import time
import os
import csv
import pandas as pd
import numpy as np


# set the time in seconds that the program stops for if the host kicks us from scraping
time = 10

# This provides a list of the promonent car brands on the website
def get_popular_brands():
	url = "https://www.cars.co.za/"
	req = requests.get("https://www.cars.co.za/")
	soup = bs(req.text,"html.parser")
	soup.prettify()
	car_table = soup.find(class_="table col5").find_all("a")
	car_links = []
	for i in car_table:
		car_links.append(i["href"])
		print(i["href"])
	pickle_on = open("popular_brands","wb")
	pickle.dump(car_links,pickle_on)
	pickle_on.close()

# get_popular_brands()

# Scrapes the data and writes it to a csv file:
# Step 1) Open popular brands
# Step 2) For each page listed per brand
#			- scrape all urls on page
# 			- If urls not in binary file (new urls):
#				- store urls in a file (binary file)
# 				- scrape data for each new url (enter url and scrape)
#				- write scraped data to brand csv

def scrape_data(popular_brands=False,car_urls_file=False):
	if popular_brands:
		brand_urls = get_popular_brands()
	else:
		open_popular_brands_pickle = open("popular_brands","rb")
		brand_urls = pickle.load(open_popular_brands_pickle)
	
	for car_brand in brand_urls:
		page_url = "https://www.cars.co.za{}".format(car_brand)

		page_request = requests.get(page_url)
		page_soup = bs(page_request.text,"html.parser")
		page_soup.prettify()
		page_find = page_soup.find(class_="resultsnum pagination__page-number pagination__page-number_right").text
		
		# Finding the total number of pages listed per brand
		total_pages = int(page_find.split("\n")[-1].strip(" "))
		
		# Drilling down into each page.
		for page in range(1,total_pages+1):

			# A try block is added incase the host kicks us. If kicked this fails and the program sleeps for a few seconds and then it continues with the next page. 

			try: 
				url_car = "https://www.cars.co.za{}?P={}".format(car_brand,page)
				request = requests.get(url_car)
				soup = bs(request.text,"html.parser")
				soup.prettify()
				cars_on_page = soup.find_all(class_ = "vehicle-list__vehicle-name")

				for car in cars_on_page:

					# another try/except block used incase a we get kicked from the site. Program will sleep for time set at top of page. 
					try: 

						# open a file with all urls, if a url already exists, we skip the scraping of the url: 
						if os.path.exists("car_urls"):
							with open("car_urls","rb") as f:
								car_urls = pickle.load(f)

						else:
							car_urls = []

						
						car = car["href"]
						# Getting the individual car details per car per page
						url = "https://www.cars.co.za{}".format(car)
						print(url)

						if url in car_urls:
							print("already scraped")

						else:

							request = requests.get(url)
							soup = bs(request.text,"html.parser")
							soup.prettify()

							# This table contains most the the details for each car.
							table = soup.find(class_="table table-bordered table-bold-col vehicle-details vehicle-view__section").find_all("td")

							# The table is an array of type of detail and then the detail e.g. Fuel Type: Petrol
							# I go down the list and grab 2 items at a time and write it to a dictionary. e.g. Fuel Type: Petrol
							car_details = {}
							incrimenter = 0
							while incrimenter < len(table) -1:
								feature_list = table[incrimenter:len(table)]
								car_details[feature_list[0].text] = feature_list[1].text
								incrimenter += 2

							# Add the brand to the car_details dictionary
							car_details["Brand"] = car_brand.split("/")[2]

							# Add the price to the car_details dictionary
							raw_price = soup.find(class_="price price_view vehicle-view__price").get_text()
							price = int("".join([(s) for s in raw_price.split() if s.isdigit()]))
							car_details["Price"] = int(price)

							# Each car page has 2 places where the car model is stated with some added indo. I want to get both and see if one is easier to clean than the other. 

							# Adding model name 1 to the car_details dictionary (name option 1)
							car_name1 = soup.find(class_="heading heading_size_xl").text
							car_details["Model_1"] = car_name1

							# Adding model name 2 to the car_details dictionary (name option 2) - This section is not alway available hence the try block
							try:
								car_name2 = soup.find(class_="heading heading_size_ml vehicle-view__description-heading vehicle-specs__heading").text
								car_details["Model_2"] = car_name2
							except:
								car_details["Model_2"] = "Unknown"

							try: 
								car_table2 = soup.find(class_="vehicle-specs__table vehicle-specs__table--bold-col")
								car_table2 = car_table2.find_all("td")
								incrimenter_t2 = 0
								while incrimenter_t2 < len(car_table2)-1:
									features = car_table2[incrimenter_t2:len(car_table2)]
									car_details[features[0].text+ "2"] = features[1].text
									incrimenter_t2 += 2

							except:
								car_detals2 = {'Engine2': 'Unknown', 'Doors2': 'Unknown', 'Transmission2': 'Unknown', 'Fuel Consumption (Average)2': 'Unknown', 'Date introduced2': 'Unknown'}
								car_details = {**car_details,**car_detals2}

							if os.path.exists("car_details.csv"): 	
								with open('car_details.csv', 'a') as f:  
									headers = car_details.keys()
									w = csv.DictWriter(f,fieldnames = headers)
									w.writerow(car_details)

							else:
								with open('car_details.csv', 'a') as f:  
									headers = car_details.keys()
									w = csv.DictWriter(f, fieldnames = headers)
									w.writeheader()
									w.writerow(car_details)


							# add the url to the scraped urls file if all went well
							car_urls.append(url)


							with open("car_urls","wb") as f:
								pickle.dump(car_urls,f)

					except:
						print("Kicked, sleep for {} seconds".format(time))
						time.sleep(time)


			except:
				print("Kicked, sleep for {} seconds".format(time))
				time.sleep(time)		



scrape_data()









