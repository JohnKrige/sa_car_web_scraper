import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns 
from bs4 import BeautifulSoup as bs
import requests
import pickle
import time



# Scraping the prices of used cars from cars.co.za
#I will be using popular brands only

# car_df = pd.DataFrame()

# To get the URLS for the popular brands to search
def get_popular_brands():
	url = "https://www.cars.co.za/"
	req = requests.get("https://www.cars.co.za/")
	soup = bs(req.text,"html.parser")
	soup.prettify()
	car_table = soup.find(class_="table col5").find_all("a")
	car_links = []
	for i in car_table:
		car_links.append(i["href"])
	pickle_on = open("popular_cars","wb")
	pickle.dump(car_links,pickle_on)
	pickle_on.close()
	

# Returns a binary file with all the links to individual entries
def get_all_car_urls(popular_brands = False):
	if popular_brands:
		car_links = get_popular_brands()
	else:
		open_pickle = open("popular_cars","rb")
		car_links = pickle.load(open_pickle)
	combined_array = []
	for link in car_links: 
		# link  = car_links[0]
		page_url = "https://www.cars.co.za{}".format(link)
		# print(url)
		page_request = requests.get(page_url)
		page_soup = bs(page_request.text,"html.parser")
		page_soup.prettify()
		page_find = page_soup.find(class_="resultsnum pagination__page-number pagination__page-number_right").text
		total_pages = int(page_find.split("\n")[-1].strip(" "))
		cars = []
		failed_pages = []
		# for page in range(1,10):
		for page in range(1,total_pages+1):
			try: 
				# print(link + ":  "+ str(page)+"/" + str(total_pages))
				url = "https://www.cars.co.za{}?P={}".format(link,page)
				print(url)
				request = requests.get(url)
				soup = bs(request.text,"html.parser")
				soup.prettify()
				cars_on_page = soup.find_all(class_ = "vehicle-list__vehicle-name")
				for i in cars_on_page:
					cars.append(i["href"])
				if page % 100 == 0:
				 	time.sleep(3)
				 	print("sleeping, another 100 pages scraped")
				                        
			except:
				time.sleep(10)
				try:
					url = "https://www.cars.co.za{}?P={}".format(link,page)
					print("except block 1")
					# print(url)
					request = requests.get(url)
					soup = bs(request.text,"html.parser")
					soup.prettify()
					cars_on_page = soup.find_all(class_ = "vehicle-list__vehicle-name")
					for i in cars_on_page:
						cars.append(i["href"])
				except:
					print("ultimate FAIL")
					failed_pages.append(url)
					time.sleep(5)


		combined_array.append([link,cars])	

		brand_name = link.split("/")[2]


		#Write the current brand to binary - incase process is cut off to avoid massive inefficiency
		pickle_now = open("car_data/pickle_{}".format(brand_name),"wb")
		pickle.dump([combined_array],pickle_now)
		pickle_now.close()
		time.sleep(10)

		#Write failed pages to binary
		pickle_fail = open("car_data/failed_pages_{}".format(brand_name),"wb")
		pickle.dump([failed_pages],pickle_fail)
		pickle_fail.close()

	pickle_on = open("all_cars","wb")
	pickle.dump([combined_array],pickle_on)
	pickle_on.close()


def creating_the_data(all_car_urls = False):
	if all_car_urls: 
		get_all_car_urls()
	else:
		pass

	open_pickle = open("all_cars","rb")
	all_car_urls = pickle.load(open_pickle)
	for i in range(5,len(all_car_urls[0])):
	# for i in range(2,3):
		car_df = pd.DataFrame()
		number_pages = 0
		failed_scrape = []
		for link in all_car_urls[0][i][1]:	
			failed_scrape = []
			try:
				number_pages += 1
				print("Scraping page: {}.....{}".format(number_pages,link))
				car_url = link
				url = "https://www.cars.co.za{}".format(car_url)
				request = requests.get(url)
				soup = bs(request.text,"html.parser")
				soup.prettify()

				#Getting the details
				table = soup.find(class_="table table-bordered table-bold-col vehicle-details vehicle-view__section").find_all("td")
				car_details = {}
				incrimenter = 0
				while incrimenter < len(table) -1:
					feature_list = table[incrimenter:len(table)]
					car_details[feature_list[0].text] = [feature_list[1].text]
					incrimenter += 2

				car_details["Brand"] = all_car_urls[0][i][0].split("/")[2]
				Brand = all_car_urls[0][i][0].split("/")[2]

				#Getting the price	
				raw_price = soup.find(class_="price price_view vehicle-view__price").get_text()
				price = int("".join([(s) for s in raw_price.split() if s.isdigit()]))
				car_details["Price"] = [price]

				#Getting the car name
				car_name = soup.find(class_="heading heading_size_xl").text
				car_details["Model"] = [car_name]


				df_car_search = pd.DataFrame.from_dict(car_details)

				if car_df.empty:
					car_df = df_car_search
				else:
					car_df = pd.concat([car_df,df_car_search],sort=True)

				if number_pages % 50 == 0:
					print("sleeping")
					print("Another 50 cars scraped")
					time.sleep(3)

			except:
				print("Exception, fail alert")
				time.sleep(10)
				failed_scrape.append(link)

		car_df.to_csv("car_data/car_csv/{}_data.csv".format(car_details["Brand"]))

		pickle_failed = open("car_data/car_csv/failed_{}_car_scrapes".format(Brand),"wb")
		pickle.dump(failed_scrape,pickle_failed)
		pickle_failed.close()

		time.sleep(10)


creating_the_data()







